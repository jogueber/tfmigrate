package tfexec

import (
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/davecgh/go-spew/spew"
	"github.com/sergi/go-diff/diffmatchpatch"
	"github.com/yudai/gojsondiff"
	"github.com/yudai/gojsondiff/formatter"
)

// TerraformPlanJSON represents the Terraform plan in JSON format
type TerraformPlanJSON struct {
	FormatVersion   string                  `json:"format_version"`
	Applyable       bool                    `json:"applyable"`
	Complete        bool                    `json:"complete"`
	Errored         bool                    `json:"errored"`
	ResourceChanges []ResourceChange        `json:"resource_changes"`
	OutputChanges   map[string]OutputChange `json:"output_changes"`
}

// ResourceChange represents a change to a resource in the plan
type ResourceChange struct {
	Address       string      `json:"address"`
	ModuleAddress string      `json:"module_address,omitempty"`
	Mode          string      `json:"mode"`
	Type          string      `json:"type"`
	Name          string      `json:"name"`
	Index         interface{} `json:"index,omitempty"`
	Deposed       string      `json:"deposed,omitempty"`
	Change        Change      `json:"change"`
	ActionReason  string      `json:"action_reason,omitempty"`
}

// OutputChange represents a change to an output value
type OutputChange struct {
	Change Change `json:"change"`
}

// Change represents the change details (before, after, actions)
type Change struct {
	Actions []string    `json:"actions"`
	Before  interface{} `json:"before"`
	After   interface{} `json:"after"`
}

// HasChanges returns true if there are any resource changes in the plan
func (p *TerraformPlanJSON) HasChanges() bool {
	hasChanges := false
	for _, rc := range p.ResourceChanges {
		// "no-op" means no changes - all other actions indicate changes
		if len(rc.Change.Actions) != 1 || rc.Change.Actions[0] != "no-op" {
			log.Printf("Change detected in resource: %s, actions: %v", rc.Address, rc.Change.Actions)
			hasChanges = true
		}
	}
	return hasChanges
}

// HasOnlyOutputChanges returns true if there are only output changes and no resource changes
func (p *TerraformPlanJSON) HasOnlyOutputChanges() bool {
	hasOutputChanges := len(p.OutputChanges) > 0

	// Check if there are any resource changes
	for _, rc := range p.ResourceChanges {
		// Any action other than "no-op" is a resource change
		if len(rc.Change.Actions) != 1 || rc.Change.Actions[0] != "no-op" {
			return false
		}
	}

	return hasOutputChanges
}

// HasOnlyCreateActions returns true if there are only create actions and no updates/deletes
func (p *TerraformPlanJSON) HasOnlyCreateActions() bool {
	for _, rc := range p.ResourceChanges {
		// Skip no-op actions
		if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "no-op" {
			continue
		}

		// Allow only create actions
		if len(rc.Change.Actions) != 1 || rc.Change.Actions[0] != "create" {
			return false
		}
	}
	return true
}

// HasOnlySafeActions returns true if there are only safe actions (create or tag-only updates)
func (p *TerraformPlanJSON) HasOnlySafeActions() bool {
	for _, rc := range p.ResourceChanges {
		// Skip no-op actions
		if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "no-op" {
			continue
		}

		// Allow create actions
		if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "create" {
			continue
		}

		// Allow update actions that are tag-only changes
		if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "update" && p.isTagOnlyChange(rc) {
			continue
		}

		// Any other action is not safe
		return false
	}
	return true
}

// isTagOnlyChange checks if a resource change only affects tags
func (p *TerraformPlanJSON) isTagOnlyChange(rc ResourceChange) bool {
	// This is a heuristic check - we look for changes that only affect tag-related fields
	// In Terraform, tag changes typically show up as changes to "tags" or "tags_all" fields

	beforeMap, beforeOk := rc.Change.Before.(map[string]interface{})
	afterMap, afterOk := rc.Change.After.(map[string]interface{})

	if !beforeOk || !afterOk {
		return false
	}

	// Check if only tag-related fields are different
	tagFields := []string{"tags", "tags_all", "tag", "user_tags", "system_tags", "default_tags"}
	onlyTagChanges := true

	// Compare all fields except tag fields
	for key, beforeVal := range beforeMap {
		afterVal, exists := afterMap[key]
		if !exists {
			// Field was removed - check if it's a tag field
			if !isTagField(key, tagFields) {
				onlyTagChanges = false
				break
			}
		} else if !reflect.DeepEqual(beforeVal, afterVal) {
			// Field was changed - check if it's a tag field
			if !isTagField(key, tagFields) {
				onlyTagChanges = false
				break
			}
		}
	}

	// Check for newly added fields
	if onlyTagChanges {
		for key := range afterMap {
			if _, exists := beforeMap[key]; !exists {
				// New field added - check if it's a tag field
				if !isTagField(key, tagFields) {
					onlyTagChanges = false
					break
				}
			}
		}
	}

	return onlyTagChanges
}

// isTagField checks if a field name is related to tags
func isTagField(fieldName string, tagFields []string) bool {
	for _, tagField := range tagFields {
		if fieldName == tagField {
			return true
		}
	}
	return false
}

func (p *TerraformPlanJSON) LogResourceChanges() {
	p.LogResourceChangesWithStatus(false, "")
}

func (p *TerraformPlanJSON) LogResourceChangesWithStatus(allowCreate bool, stateType string) {
	if len(p.ResourceChanges) == 0 {
		log.Printf("No resource changes detected")
		return
	}

	log.Printf("\n🔍 RESOURCE CHANGES DETECTED:")
	log.Printf("═══════════════════════════════════════════════════════════")

	for i, rc := range p.ResourceChanges {
		// Skip resources with "no-op" actions
		if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "no-op" {
			continue
		}

		// Determine if this individual change would be accepted
		var statusEmoji, statusText string
		if stateType != "" {
			if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "create" && allowCreate {
				statusEmoji = "✅"
				statusText = "ACCEPTED"
			} else if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "create" && !allowCreate {
				statusEmoji = "❌"
				statusText = "REJECTED (create not allowed in source state)"
			} else if len(rc.Change.Actions) == 1 && rc.Change.Actions[0] == "update" && p.isTagOnlyChange(rc) {
				statusEmoji = "✅"
				statusText = "ACCEPTED (tag-only change)"
			} else {
				statusEmoji = "❌"
				statusText = fmt.Sprintf("REJECTED (non-safe action in %s state)", stateType)
			}
		}

		log.Printf("\n📦 Resource #%d:", i+1)
		log.Printf("┌─────────────────────────────────────────────────────────")
		log.Printf("│ Address: %s", rc.Address)
		log.Printf("│ Type: %s", rc.Type)
		log.Printf("│ Mode: %s", rc.Mode)
		log.Printf("│ Actions: %v", formatActions(rc.Change.Actions))

		if stateType != "" {
			log.Printf("│ Status: %s %s", statusEmoji, statusText)
		}

		if rc.Index != nil {
			log.Printf("│ Index: %v", rc.Index)
		}

		if rc.ActionReason != "" {
			log.Printf("│ Reason: %s", rc.ActionReason)
		}

		// Show the actual changes
		if !reflect.DeepEqual(rc.Change.Before, rc.Change.After) {
			log.Printf("│")
			log.Printf("│ 🔄 Changes:")
			changeLines := strings.Split(createDiff(rc.Change.Before, rc.Change.After, "Value"), "\n")
			for _, line := range changeLines {
				if strings.TrimSpace(line) != "" {
					log.Printf("│ %s", line)
				}
			}
		}

		log.Printf("└─────────────────────────────────────────────────────────")
	}

	log.Printf("\n═══════════════════════════════════════════════════════════")
}

// formatActions formats the action list with emojis for better readability
func formatActions(actions []string) string {
	var formatted []string
	for _, action := range actions {
		switch action {
		case "create":
			formatted = append(formatted, "➕ create")
		case "update":
			formatted = append(formatted, "🔄 update")
		case "delete":
			formatted = append(formatted, "❌ delete")
		case "replace":
			formatted = append(formatted, "🔄 replace")
		case "no-op":
			formatted = append(formatted, "⚪ no-op")
		default:
			formatted = append(formatted, action)
		}
	}
	return "[" + strings.Join(formatted, ", ") + "]"
}

func (p *TerraformPlanJSON) LogOutputChanges() {
	if len(p.OutputChanges) == 0 {
		log.Printf("No output changes detected")
		return
	}

	log.Printf("\n📤 OUTPUT CHANGES DETECTED:")
	log.Printf("═══════════════════════════════════════════════════════════")

	for name, oc := range p.OutputChanges {
		log.Printf("\n📋 Output: %s", name)
		log.Printf("┌─────────────────────────────────────────────────────────")
		log.Printf("│ Actions: %v", formatActions(oc.Change.Actions))

		// Show the actual changes
		if !reflect.DeepEqual(oc.Change.Before, oc.Change.After) {
			log.Printf("│")
			log.Printf("│ 🔄 Changes:")
			changeLines := strings.Split(createDiff(oc.Change.Before, oc.Change.After, "Value"), "\n")
			for _, line := range changeLines {
				if strings.TrimSpace(line) != "" {
					log.Printf("│ %s", line)
				}
			}
		}

		log.Printf("└─────────────────────────────────────────────────────────")
	}

	log.Printf("\n═══════════════════════════════════════════════════════════")
}

// formatValue formats a value for readable display in diffs
func formatValue(value interface{}) string {
	if value == nil {
		return "<nil>"
	}

	// Try to format as JSON first for structured data
	if jsonBytes, err := json.MarshalIndent(value, "", "  "); err == nil {
		jsonStr := string(jsonBytes)
		// If it's a simple value (no newlines), keep it on one line
		if !strings.Contains(jsonStr, "\n") {
			return jsonStr
		}
		// For complex structures, use pretty JSON
		return jsonStr
	}

	// Fallback to spew for Go types that don't marshal well to JSON
	return spew.Sdump(value)
}

// createDiff creates a readable diff between two values using appropriate libraries
func createDiff(before, after interface{}, label string) string {
	if reflect.DeepEqual(before, after) {
		return fmt.Sprintf("    %s: (no change) %s", label, formatValue(before))
	}

	// Try JSON diff first for structured data
	if jsonDiff := createJSONDiff(before, after, label); jsonDiff != "" {
		return jsonDiff
	}

	// Fallback to text diff
	return createTextDiff(before, after, label)
}

// createJSONDiff attempts to create a JSON diff if both values can be marshaled to JSON
func createJSONDiff(before, after interface{}, label string) string {
	beforeJSON, beforeErr := json.Marshal(before)
	afterJSON, afterErr := json.Marshal(after)

	if beforeErr != nil || afterErr != nil {
		return "" // Can't create JSON diff, fallback to text diff
	}

	// Use gojsondiff for JSON comparison
	differ := gojsondiff.New()
	diff, err := differ.Compare(beforeJSON, afterJSON)
	if err != nil {
		return "" // Can't create JSON diff, fallback to text diff
	}

	if !diff.Modified() {
		return fmt.Sprintf("    %s: (no change)", label)
	}

	// Create a nice ASCII formatter
	config := formatter.AsciiFormatterConfig{
		ShowArrayIndex: true,
		Coloring:       false, // Disable coloring for log output
	}

	formatterInstance := formatter.NewAsciiFormatter(json.RawMessage{}, config)
	diffString, err := formatterInstance.Format(diff)
	if err != nil {
		return "" // Can't format, fallback to text diff
	}

	return fmt.Sprintf("    %s (JSON diff):\n%s", label, indentLines(diffString, "      "))
}

// createTextDiff creates a text-based diff using diffmatchpatch
func createTextDiff(before, after interface{}, label string) string {
	beforeStr := formatValue(before)
	afterStr := formatValue(after)

	// For simple single-line values, show them inline
	if !strings.Contains(beforeStr, "\n") && !strings.Contains(afterStr, "\n") {
		return fmt.Sprintf("    %s:\n      - %s\n      + %s", label, beforeStr, afterStr)
	}

	// For complex multi-line values, use diffmatchpatch
	dmp := diffmatchpatch.New()
	diffs := dmp.DiffMain(beforeStr, afterStr, false)
	diffs = dmp.DiffCleanupSemantic(diffs)

	prettyDiff := dmp.DiffPrettyText(diffs)

	return fmt.Sprintf("    %s (text diff):\n%s", label, indentLines(prettyDiff, "      "))
}

// indentLines adds indentation to each line of a multi-line string
func indentLines(text, indent string) string {
	lines := strings.Split(text, "\n")
	for i, line := range lines {
		if strings.TrimSpace(line) != "" {
			lines[i] = indent + line
		}
	}
	return strings.Join(lines, "\n")
}
