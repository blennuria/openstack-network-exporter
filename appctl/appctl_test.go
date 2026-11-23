// SPDX-License-Identifier: Apache-2.0

package appctl

import (
	"os"
	"path/filepath"
	"testing"
)

func TestFindCtlSocket(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		want    string
		wantErr bool
	}{
		{
			name: "pid file and pid socket",
			files: map[string]string{
				"ovn-northd.pid":    "42\n",
				"ovn-northd.42.ctl": "",
			},
			want: "ovn-northd.42.ctl",
		},
		{
			name: "pid socket without pid file",
			files: map[string]string{
				"ovn-northd.42.ctl": "",
			},
			want: "ovn-northd.42.ctl",
		},
		{
			name: "pid file and fixed socket",
			files: map[string]string{
				"ovn-northd.pid": "7\n",
				"ovn-northd.ctl": "",
			},
			want: "ovn-northd.ctl",
		},
		{
			name: "fixed socket without pid file",
			files: map[string]string{
				"ovn-northd.ctl": "",
			},
			want: "ovn-northd.ctl",
		},
		{
			name: "pid socket is preferred to the fixed one",
			files: map[string]string{
				"ovn-northd.pid":    "42\n",
				"ovn-northd.42.ctl": "",
				"ovn-northd.ctl":    "",
			},
			want: "ovn-northd.42.ctl",
		},
		{
			name: "pid file without any socket",
			files: map[string]string{
				"ovn-northd.pid": "42\n",
			},
			want: "ovn-northd.42.ctl",
		},
		{
			name: "socket of another daemon",
			files: map[string]string{
				"ovn-controller.pid":    "42\n",
				"ovn-controller.42.ctl": "",
				"ovn-controller.ctl":    "",
			},
			wantErr: true,
		},
		{
			name:    "empty directory",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rundir := t.TempDir()
			for name, content := range tt.files {
				if err := os.WriteFile(filepath.Join(rundir, name), []byte(content), 0644); err != nil {
					t.Fatal(err)
				}
			}

			got, err := findCtlSocket(rundir, ovnNorthd)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected an error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if want := filepath.Join(rundir, tt.want); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
}
