package command

import (
	"context"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/minamijoyo/tfmigrate/config"
	"github.com/minamijoyo/tfmigrate/history"
	"github.com/minamijoyo/tfmigrate/storage/mock"
)

func TestHistoryRunnerPlan(t *testing.T) {
	cases := []struct {
		desc        string
		migrations  map[string]string
		historyFile string
		filename    string
		want        string
		ok          bool
	}{
		{
			desc: "no args",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000004_test4.hcl": `
migration "mock" "test4" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename: "",
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "a filename is given",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test4.hcl": `
migration "mocr" "test4" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename: "20201109000003_test3.hcl",
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "partial success (plan error)",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000004_test4.hcl": `
migration "mock" "test4" {
	plan_error  = true
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename: "",
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			ok: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			migrationDir := setupMigrationDir(t, tc.migrations)
			mockConfig := &mock.Config{
				Data:       tc.historyFile,
				WriteError: false,
				ReadError:  false,
			}
			config := &config.TfmigrateConfig{
				MigrationDir: migrationDir,
				History: &history.Config{
					Storage: mockConfig,
				},
			}
			r, err := NewHistoryRunner(context.Background(), tc.filename, config, nil)
			if err != nil {
				t.Fatalf("failed to new history runner: %s", err)
			}

			err = r.Plan(context.Background())
			if tc.ok && err != nil {
				t.Fatalf("unexpected err: %s", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("expected to return an error, but no error")
			}
			want, err := history.ParseHistoryFile([]byte(tc.want))
			if err != nil {
				t.Fatalf("failed to parse history file (want): %s", err)
			}
			data := mockConfig.Storage().Data()
			got, err := history.ParseHistoryFile([]byte(data))
			if err != nil {
				t.Fatalf("failed to parse history file (got): %s", err)
			}
			recordObj := history.Record{}
			if diff := cmp.Diff(*got, *want, cmp.AllowUnexported(*got), cmpopts.IgnoreFields(recordObj, "AppliedAt")); diff != "" {
				t.Errorf("got = %#v, want = %#v, diff = %s", got, want, diff)
			}
		})
	}
}

func TestHistoryRunnerApply(t *testing.T) {
	cases := []struct {
		desc        string
		migrations  map[string]string
		historyFile string
		filename    string
		writeError  bool
		readError   bool
		want        string
		ok          bool
	}{
		{
			desc: "no args",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000004_test4.hcl": `
migration "mock" "test4" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename:   "",
			writeError: false,
			readError:  false,
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        },
        "20201109000003_test3.hcl": {
            "type": "mock",
            "name": "test3",
            "applied_at": "2020-11-10T00:00:03Z"
        },
        "20201109000004_test4.hcl": {
            "type": "mock",
            "name": "test4",
            "applied_at": "2020-11-10T00:00:04Z"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "no unapplied migrations",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename:   "",
			writeError: false,
			readError:  false,
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "a filename is given",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test4.hcl": `
migration "mocr" "test4" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename:   "20201109000003_test3.hcl",
			writeError: false,
			readError:  false,
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        },
        "20201109000003_test3.hcl": {
            "type": "mock",
            "name": "test3",
            "applied_at": "2020-11-10T00:00:03Z"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "a migration has already been applied",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test4.hcl": `
migration "mocr" "test4" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename:   "20201109000002_test2.hcl",
			writeError: false,
			readError:  false,
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			ok: false,
		},
		{
			desc: "partial apply error but save history success",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000004_test4.hcl": `
migration "mock" "test4" {
	plan_error  = false
	apply_error = true
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename:   "",
			writeError: false,
			readError:  false,
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        },
        "20201109000003_test3.hcl": {
            "type": "mock",
            "name": "test3",
            "applied_at": "2020-11-10T00:00:03Z"
        }
    }
}`,
			ok: false,
		},
		{
			desc: "apply success but save history error",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000004_test4.hcl": `
migration "mock" "test4" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename:   "",
			writeError: true,
			readError:  false,
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			ok: false,
		},
		{
			desc: "apply error and save history error",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test3" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000004_test4.hcl": `
migration "mock" "test4" {
	plan_error  = false
	apply_error = true
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			filename:   "",
			writeError: true,
			readError:  false,
			want: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			ok: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			migrationDir := setupMigrationDir(t, tc.migrations)
			mockConfig := &mock.Config{
				Data:       tc.historyFile,
				WriteError: tc.writeError,
				ReadError:  tc.readError,
			}
			config := &config.TfmigrateConfig{
				MigrationDir: migrationDir,
				History: &history.Config{
					Storage: mockConfig,
				},
			}
			r, err := NewHistoryRunner(context.Background(), tc.filename, config, nil)
			if err != nil {
				t.Fatalf("failed to new history runner: %s", err)
			}

			err = r.Apply(context.Background())
			if tc.ok && err != nil {
				t.Fatalf("unexpected err: %s", err)
			}
			if !tc.ok && err == nil {
				t.Fatal("expected to return an error, but no error")
			}
			want, err := history.ParseHistoryFile([]byte(tc.want))
			if err != nil {
				t.Fatalf("failed to parse history file (want): %s", err)
			}
			data := mockConfig.Storage().Data()
			got, err := history.ParseHistoryFile([]byte(data))
			if err != nil {
				t.Fatalf("failed to parse history file (got): %s", err)
			}
			recordObj := history.Record{}
			if diff := cmp.Diff(*got, *want, cmp.AllowUnexported(*got), cmpopts.IgnoreFields(recordObj, "AppliedAt")); diff != "" {
				t.Errorf("got = %#v, want = %#v, diff = %s", got, want, diff)
			}
		})
	}
}

func TestHistoryRunnerValidateDuplicateMigrations(t *testing.T) {
	cases := []struct {
		desc        string
		migrations  map[string]string
		historyFile string
		wantErr     string
		ok          bool
	}{
		{
			desc: "no duplicates",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000003_test3.hcl": {
            "type": "mock",
            "name": "test3",
            "applied_at": "2020-11-10T00:00:01Z"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "duplicate migration names in local files",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {}
}`,
			wantErr: "duplicate migration names found locally:",
			ok:      false,
		},
		{
			desc: "multiple duplicate migration names in local files",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000003_test3.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000004_test4.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {}
}`,
			wantErr: "duplicate migration names found locally:",
			ok:      false,
		},
		{
			desc: "duplicate migration name in history",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000003_test3.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        }
    }
}`,
			wantErr: "duplicate migration names found in remote state:",
			ok:      false,
		},
		{
			desc: "multiple duplicate migration names in history",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000003_test3.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        },
        "20201109000004_test4.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z"
        }
    }
}`,
			wantErr: "duplicate migration names found in remote state:",
			ok:      false,
		},
		{
			desc: "local file with same name but different migration content",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000001_duplicate.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {}
}`,
			ok: true,
		},
		{
			desc: "invalid migration file syntax",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_invalid.hcl": `
invalid syntax here
`,
			},
			historyFile: `{
    "version": 1,
    "records": {}
}`,
			wantErr: "failed to parse migration file",
			ok:      false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			migrationDir := setupMigrationDir(t, tc.migrations)
			mockConfig := &mock.Config{
				Data:       tc.historyFile,
				WriteError: false,
				ReadError:  false,
			}
			config := &config.TfmigrateConfig{
				MigrationDir: migrationDir,
				History: &history.Config{
					Storage: mockConfig,
				},
			}

			// Create a history runner (we don't need to specify a filename for validation)
			r, err := NewHistoryRunner(context.Background(), "", config, nil)
			if err != nil {
				t.Fatalf("failed to new history runner: %s", err)
			}

			// Test the validateNoDuplicates method
			err = r.validateNoDuplicates(context.Background())
			
			if tc.ok {
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
			} else {
				if err == nil {
					t.Fatal("expected to return an error, but no error")
				}
				if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected error to contain %q, but got: %s", tc.wantErr, err.Error())
				}
			}
		})
	}
}

func TestHistoryRunnerPlanWithDuplicateMigrations(t *testing.T) {
	cases := []struct {
		desc        string
		migrations  map[string]string
		historyFile string
		filename    string
		wantErr     string
		ok          bool
	}{
		{
			desc: "plan should fail with local duplicates",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {}
}`,
			filename: "", // directory mode to trigger validation
			wantErr:  "duplicate migration names found locally:",
			ok:       false,
		},
		{
			desc: "plan should fail with remote duplicates",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        }
    }
}`,
			filename: "", // directory mode to trigger validation
			wantErr:  "duplicate migration names found in remote state:",
			ok:       false,
		},
		{
			desc: "apply should fail with local duplicates",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {}
}`,
			filename: "", // directory mode to trigger validation
			wantErr:  "duplicate migration names found locally:",
			ok:       false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			migrationDir := setupMigrationDir(t, tc.migrations)
			mockConfig := &mock.Config{
				Data:       tc.historyFile,
				WriteError: false,
				ReadError:  false,
			}
			config := &config.TfmigrateConfig{
				MigrationDir: migrationDir,
				History: &history.Config{
					Storage: mockConfig,
				},
			}

			r, err := NewHistoryRunner(context.Background(), tc.filename, config, nil)
			if err != nil {
				t.Fatalf("failed to new history runner: %s", err)
			}

			// Test both Plan and Apply methods
			if strings.Contains(tc.desc, "plan") {
				err = r.Plan(context.Background())
			} else {
				err = r.Apply(context.Background())
			}

			if tc.ok {
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
			} else {
				if err == nil {
					t.Fatal("expected to return an error, but no error")
				}
				if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected error to contain %q, but got: %s", tc.wantErr, err.Error())
				}
			}
		})
	}
}

func TestHistoryRunnerValidateMD5Hashes(t *testing.T) {
	cases := []struct {
		desc        string
		migrations  map[string]string
		historyFile string
		wantErr     string
		ok          bool
	}{
		{
			desc: "valid MD5 hashes",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z",
            "md5_hash": "dd5fa8b7b42e65b11b3a6e42ecdc7fd1"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "MD5 hash mismatch (file modified after application)",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = true
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z",
            "md5_hash": "dd5fa8b7b42e65b11b3a6e42ecdc7fd1"
        }
    }
}`,
			wantErr: "migration file integrity check failed:",
			ok:      false,
		},
		{
			desc: "duplicate MD5 hashes in history",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z",
            "md5_hash": "dd5fa8b7b42e65b11b3a6e42ecdc7fd1"
        },
        "20201109000002_test2.hcl": {
            "type": "mock",
            "name": "test2",
            "applied_at": "2020-11-10T00:00:02Z",
            "md5_hash": "dd5fa8b7b42e65b11b3a6e42ecdc7fd1"
        }
    }
}`,
			wantErr: "duplicate migration content found in history:",
			ok:      false,
		},
		{
			desc: "missing MD5 hash in history (backward compatibility)",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z"
        }
    }
}`,
			ok: true,
		},
		{
			desc: "new migration with valid content",
			migrations: map[string]string{
				"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
				"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
			},
			historyFile: `{
    "version": 1,
    "records": {
        "20201109000001_test1.hcl": {
            "type": "mock",
            "name": "test1",
            "applied_at": "2020-11-10T00:00:01Z",
            "md5_hash": "dd5fa8b7b42e65b11b3a6e42ecdc7fd1"
        }
    }
}`,
			ok: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.desc, func(t *testing.T) {
			migrationDir := setupMigrationDir(t, tc.migrations)
			mockConfig := &mock.Config{
				Data:       tc.historyFile,
				WriteError: false,
				ReadError:  false,
			}
			config := &config.TfmigrateConfig{
				MigrationDir: migrationDir,
				History: &history.Config{
					Storage: mockConfig,
				},
			}

			// Create a history runner
			r, err := NewHistoryRunner(context.Background(), "", config, nil)
			if err != nil {
				t.Fatalf("failed to new history runner: %s", err)
			}

			// Test the validateNoDuplicates method which now includes MD5 validation
			err = r.validateNoDuplicates(context.Background())
			
			if tc.ok {
				if err != nil {
					t.Fatalf("unexpected error: %s", err)
				}
			} else {
				if err == nil {
					t.Fatal("expected to return an error, but no error")
				}
				if tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("expected error to contain %q, but got: %s", tc.wantErr, err.Error())
				}
			}
		})
	}
}

func TestHistoryRunnerApplyWithMD5Hash(t *testing.T) {
	migrations := map[string]string{
		"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
	}

	migrationDir := setupMigrationDir(t, migrations)
	mockConfig := &mock.Config{
		Data:       `{"version": 1, "records": {}}`,
		WriteError: false,
		ReadError:  false,
	}
	config := &config.TfmigrateConfig{
		MigrationDir: migrationDir,
		History: &history.Config{
			Storage: mockConfig,
		},
	}

	r, err := NewHistoryRunner(context.Background(), "20201109000001_test1.hcl", config, nil)
	if err != nil {
		t.Fatalf("failed to new history runner: %s", err)
	}

	// Apply the migration
	err = r.Apply(context.Background())
	if err != nil {
		t.Fatalf("unexpected error during apply: %s", err)
	}

	// Get records through the controller
	records := r.hc.Records()
	record, exists := records["20201109000001_test1.hcl"]
	if !exists {
		t.Fatal("migration record not found in history")
	}

	if record.MD5Hash == "" {
		t.Error("MD5 hash not stored in history record")
	}

	// Verify the MD5 hash is correct
	expectedHash := "dd5fa8b7b42e65b11b3a6e42ecdc7fd1" // hash of the migration content
	if record.MD5Hash != expectedHash {
		t.Errorf("expected MD5 hash %s, got %s", expectedHash, record.MD5Hash)
	}
}

func TestHistoryRunnerBackwardCompatibilityMD5Hash(t *testing.T) {
	migrations := map[string]string{
		"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
		"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
	}

	migrationDir := setupMigrationDir(t, migrations)

	// Create history with missing MD5 hashes (old format)
	historyWithoutMD5 := `{
		"version": 1,
		"records": {
			"20201109000001_test1.hcl": {
				"type": "mock",
				"name": "test1",
				"applied_at": "2020-11-09T00:00:01Z"
			}
		}
	}`

	mockConfig := &mock.Config{
		Data:       historyWithoutMD5,
		WriteError: false,
		ReadError:  false,
	}
	config := &config.TfmigrateConfig{
		MigrationDir: migrationDir,
		History: &history.Config{
			Storage: mockConfig,
		},
	}

	r, err := NewHistoryRunner(context.Background(), "", config, nil)
	if err != nil {
		t.Fatalf("failed to new history runner: %s", err)
	}

	// Verify that validation passes with missing MD5 hash (backward compatibility)
	err = r.validateNoDuplicates(context.Background())
	if err != nil {
		t.Fatalf("validation should pass with missing MD5 hash for backward compatibility: %s", err)
	}

	// Test updateMissingMD5Hashes method
	err = r.updateMissingMD5Hashes(context.Background())
	if err != nil {
		t.Fatalf("failed to update missing MD5 hashes: %s", err)
	}

	// Verify that MD5 hash was added to the existing record
	records := r.hc.Records()
	record, exists := records["20201109000001_test1.hcl"]
	if !exists {
		t.Fatal("migration record not found in history")
	}

	if record.MD5Hash == "" {
		t.Error("MD5 hash should have been added to existing record")
	}

	expectedHash := "dd5fa8b7b42e65b11b3a6e42ecdc7fd1" // hash of the migration content
	if record.MD5Hash != expectedHash {
		t.Errorf("expected MD5 hash %s, got %s", expectedHash, record.MD5Hash)
	}

	// Apply another migration to verify the flow works with mixed records
	err = r.Apply(context.Background())
	if err != nil {
		t.Fatalf("unexpected error during apply: %s", err)
	}

	// Verify both records have MD5 hashes now
	records = r.hc.Records()
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}

	for filename, record := range records {
		if record.MD5Hash == "" {
			t.Errorf("record %s should have MD5 hash", filename)
		}
	}
}

func TestHistoryRunnerUpdateMissingMD5HashesWithMissingFile(t *testing.T) {
	migrations := map[string]string{
		"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
	}

	migrationDir := setupMigrationDir(t, migrations)

	// Create history with a record for a file that no longer exists
	historyWithMissingFile := `{
		"version": 1,
		"records": {
			"20201109000001_test1.hcl": {
				"type": "mock",
				"name": "test1",
				"applied_at": "2020-11-09T00:00:01Z"
			},
			"20201109000099_missing.hcl": {
				"type": "mock",
				"name": "missing",
				"applied_at": "2020-11-09T00:00:01Z"
			}
		}
	}`

	mockConfig := &mock.Config{
		Data:       historyWithMissingFile,
		WriteError: false,
		ReadError:  false,
	}
	config := &config.TfmigrateConfig{
		MigrationDir: migrationDir,
		History: &history.Config{
			Storage: mockConfig,
		},
	}

	r, err := NewHistoryRunner(context.Background(), "", config, nil)
	if err != nil {
		t.Fatalf("failed to new history runner: %s", err)
	}

	// Test updateMissingMD5Hashes with missing file
	err = r.updateMissingMD5Hashes(context.Background())
	if err != nil {
		t.Fatalf("updateMissingMD5Hashes should not fail when file is missing: %s", err)
	}

	// Verify that only the existing file got its MD5 hash updated
	records := r.hc.Records()
	
	record1, exists := records["20201109000001_test1.hcl"]
	if !exists {
		t.Fatal("migration record test1 not found in history")
	}
	if record1.MD5Hash == "" {
		t.Error("existing file should have MD5 hash updated")
	}

	recordMissing, exists := records["20201109000099_missing.hcl"]
	if !exists {
		t.Fatal("migration record for missing file not found in history")
	}
	if recordMissing.MD5Hash != "" {
		t.Error("missing file record should not have MD5 hash updated")
	}
}

func TestHistoryRunnerValidationIgnoresEmptyMD5Hash(t *testing.T) {
	migrations := map[string]string{
		"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
		"20201109000002_test2.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
	}

	migrationDir := setupMigrationDir(t, migrations)

	// Create history with mixed records - some with MD5, some without
	historyMixed := `{
		"version": 1,
		"records": {
			"20201109000001_test1.hcl": {
				"type": "mock",
				"name": "test1",
				"applied_at": "2020-11-09T00:00:01Z"
			},
			"20201109000002_test2.hcl": {
				"type": "mock",
				"name": "test1",
				"md5_hash": "dd5fa8b7b42e65b11b3a6e42ecdc7fd1",
				"applied_at": "2020-11-09T00:00:02Z"
			}
		}
	}`

	mockConfig := &mock.Config{
		Data:       historyMixed,
		WriteError: false,
		ReadError:  false,
	}
	config := &config.TfmigrateConfig{
		MigrationDir: migrationDir,
		History: &history.Config{
			Storage: mockConfig,
		},
	}

	r, err := NewHistoryRunner(context.Background(), "", config, nil)
	if err != nil {
		t.Fatalf("failed to new history runner: %s", err)
	}

	// This should detect duplicate migration names but ignore the missing MD5 hash
	err = r.validateNoDuplicates(context.Background())
	if err == nil {
		t.Fatal("expected error for duplicate migration names")
	}

	if !strings.Contains(err.Error(), "duplicate migration name") {
		t.Errorf("expected error about duplicate migration names, got: %s", err.Error())
	}

	// Should not contain error about MD5 hash validation since one record has empty hash
	if strings.Contains(err.Error(), "MD5") {
		t.Errorf("should not validate MD5 for records with empty hash, got: %s", err.Error())
	}
}

func TestHistoryRunnerPlanDirWithBackwardCompatibility(t *testing.T) {
	migrations := map[string]string{
		"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
		"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
	}

	migrationDir := setupMigrationDir(t, migrations)

	// Create history with old records without MD5 hashes
	historyWithoutMD5 := `{
		"version": 1,
		"records": {
			"20201109000001_test1.hcl": {
				"type": "mock",
				"name": "test1",
				"applied_at": "2020-11-09T00:00:01Z"
			}
		}
	}`

	mockConfig := &mock.Config{
		Data:       historyWithoutMD5,
		WriteError: false,
		ReadError:  false,
	}
	config := &config.TfmigrateConfig{
		MigrationDir: migrationDir,
		History: &history.Config{
			Storage: mockConfig,
		},
	}

	r, err := NewHistoryRunner(context.Background(), "", config, nil)
	if err != nil {
		t.Fatalf("failed to new history runner: %s", err)
	}

	// Plan should work and update missing MD5 hashes
	err = r.Plan(context.Background())
	if err != nil {
		t.Fatalf("plan should succeed with backward compatibility: %s", err)
	}

	// Verify that MD5 hash was added during planning
	records := r.hc.Records()
	record, exists := records["20201109000001_test1.hcl"]
	if !exists {
		t.Fatal("migration record not found in history")
	}

	if record.MD5Hash == "" {
		t.Error("MD5 hash should have been added during planning")
	}
}

func TestHistoryRunnerApplyDirWithBackwardCompatibility(t *testing.T) {
	migrations := map[string]string{
		"20201109000001_test1.hcl": `
migration "mock" "test1" {
	plan_error  = false
	apply_error = false
}
`,
		"20201109000002_test2.hcl": `
migration "mock" "test2" {
	plan_error  = false
	apply_error = false
}
`,
	}

	migrationDir := setupMigrationDir(t, migrations)

	// Create history with old records without MD5 hashes
	historyWithoutMD5 := `{
		"version": 1,
		"records": {
			"20201109000001_test1.hcl": {
				"type": "mock",
				"name": "test1",
				"applied_at": "2020-11-09T00:00:01Z"
			}
		}
	}`

	mockConfig := &mock.Config{
		Data:       historyWithoutMD5,
		WriteError: false,
		ReadError:  false,
	}
	config := &config.TfmigrateConfig{
		MigrationDir: migrationDir,
		History: &history.Config{
			Storage: mockConfig,
		},
	}

	r, err := NewHistoryRunner(context.Background(), "", config, nil)
	if err != nil {
		t.Fatalf("failed to new history runner: %s", err)
	}

	// Apply should work and update missing MD5 hashes
	err = r.Apply(context.Background())
	if err != nil {
		t.Fatalf("apply should succeed with backward compatibility: %s", err)
	}

	// Verify that all records now have MD5 hashes
	records := r.hc.Records()
	if len(records) != 2 {
		t.Errorf("expected 2 records, got %d", len(records))
	}

	for filename, record := range records {
		if record.MD5Hash == "" {
			t.Errorf("record %s should have MD5 hash after apply", filename)
		}
	}

	// Verify the old record got updated
	record1, exists := records["20201109000001_test1.hcl"]
	if !exists {
		t.Fatal("migration record test1 not found in history")
	}
	expectedHash := "dd5fa8b7b42e65b11b3a6e42ecdc7fd1"
	if record1.MD5Hash != expectedHash {
		t.Errorf("expected MD5 hash %s for updated record, got %s", expectedHash, record1.MD5Hash)
	}
}
