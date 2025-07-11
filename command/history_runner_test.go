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
