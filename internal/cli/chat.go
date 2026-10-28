// Copyright 2026 The A2A Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/spf13/cobra"

	"github.com/a2aproject/a2a-cli/internal/chat"
	"github.com/a2aproject/a2a-cli/internal/clierr"
	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
)

type chatFlags struct {
	stream       bool
	taskID       string
	contextID    string
	pollInterval time.Duration
}

func newChatCmd(cfg *globalConfig, deps deps) *cobra.Command {
	var flags chatFlags

	cmd := &cobra.Command{
		Use:   "chat [message]",
		Short: "Chat with an agent, carrying the conversation across turns",
		Long: "Chat with an agent interactively. Each reply continues the same conversation: the context id is carried " +
			"across turns, and an input-required task is continued with your next message. Type " + chat.Quit + " or press " +
			"Ctrl-D to exit; the context and task ids are printed so you can resume with send, or with chat --context-id and --task-id. " +
			"Ctrl-C cancels a running turn and asks the agent to cancel its task, or exits at the prompt. " +
			"A turn that exceeds --timeout also cancels its task.\n\n" +
			"When standard input or output is not a terminal, chat sends [message] once, prints the context and task ids, " +
			"and reads no input.",
		Args: cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			interactive := deps.isTerminal()
			if !interactive && len(args) == 0 {
				return clierr.Usage("chat needs a terminal; pass a message to send it once, or use send")
			}

			baseCtx := withServiceParams(cmd.Context(), cfg)
			ctx, cancel := context.WithTimeout(baseCtx, cfg.timeout)
			client, err := newAgentClient(ctx, cfg)
			cancel()
			if err != nil {
				return fmt.Errorf("failed to create a client: %w", err)
			}
			defer destroyClient(cfg, client)

			session := chat.NewSession(chat.Config{
				Send: func(ctx context.Context, msg *a2a.Message) error {
					return sendChatMessage(ctx, cfg, client, &flags, deps.poller, msg)
				},
				Cancel: func(ctx context.Context, id a2a.TaskID) error {
					ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
					defer cancel()
					_, err := client.CancelTask(ctx, &a2a.CancelTaskRequest{ID: id, Tenant: cfg.tenant})
					return err
				},
				RenderError: func(err error) { cfg.renderError(err) },
				Notify:      deps.notifyInterrupt,
				Out:         cfg.stderr(),
			}, flags.contextID, a2a.TaskID(flags.taskID))
			cfg.handlers = append(cfg.handlers, session.Track)

			if !interactive {
				return session.Once(baseCtx, args[0])
			}
			var first string
			if len(args) == 1 {
				first = args[0]
			}
			return session.Run(baseCtx, cmd.InOrStdin(), first)
		},
	}

	f := cmd.Flags()
	f.BoolVar(&flags.stream, "stream", false, "Use streaming response. Falls back to polling and synthetic events if a server does not support streaming.")
	f.StringVar(&flags.taskID, "task-id", "", "Task ID to continue an existing task")
	f.StringVar(&flags.contextID, "context-id", "", "Context ID to group this turn under")
	f.DurationVar(&flags.pollInterval, "poll-interval", 2*time.Second, "Duration between GetTask requests in polling fallback mode.")

	return cmd
}

func sendChatMessage(ctx context.Context, cfg *globalConfig, client *a2aclient.Client, flags *chatFlags, poller pollerFunc, msg *a2a.Message) error {
	req := &a2a.SendMessageRequest{Message: msg, Tenant: cfg.tenant}
	if flags.stream {
		return sendStreaming(ctx, cfg, client, req, poller, flags.pollInterval)
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.timeout)
	defer cancel()
	result, err := client.SendMessage(ctx, req)
	if err != nil {
		return fmt.Errorf("failed to send message: %w", err)
	}
	return cfg.handleEvent(result)
}

// stdioIsTerminal reports whether both standard input and output are terminals.
func stdioIsTerminal() bool {
	return isCharDevice(os.Stdin) && isCharDevice(os.Stdout)
}

func isCharDevice(f *os.File) bool {
	info, err := f.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}
