// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package topic

import (
	"context"
	"os"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/kafka"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func NewReassignPartitionCommand(fs afero.Fs) *cobra.Command {
	var (
		topic     string
		partition int32
		brokerId  int32
	)
	cmd := &cobra.Command{
		Use:   "reassign-partition [TOPICS...]",
		Short: "Reassign partition",
		Args:  cobra.MinimumNArgs(0),
		Long: `Reassign partition.

Transfers partition to specified broker under a topic.
`,

		Run: func(cmd *cobra.Command, topics []string) {
			p := config.ParamsFromCommand(cmd)
			cfg, err := p.Load(fs)
			out.MaybeDie(err, "unable to load config: %v", err)

			cl, err := kafka.NewFranzClient(fs, p, cfg)
			out.MaybeDie(err, "unable to initialize kafka client: %v", err)
			defer cl.Close()

			req := kmsg.NewPtrAlterPartitionAssignmentsRequest()
			req.Topics = []kmsg.AlterPartitionAssignmentsRequestTopic{
				{
					Topic: topic,
					Partitions: []kmsg.AlterPartitionAssignmentsRequestTopicPartition{
						{
							Partition: partition,
						},
					},
				},
			}

			resp, err := req.RequestWith(context.Background(), cl)
			out.MaybeDie(err, "unable to reassign partition %d on topic %s: %v", partition, topic, err)

			var exit1 bool
			defer func() {
				if exit1 {
					os.Exit(1)
				}
			}()

			tw := out.NewTable("topic", "status")
			defer tw.Flush()

			msg := "OK"
			if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
				msg = err.Error()
			}
			tw.Print(msg)
		},
	}
	cmd.Flags().StringVarP(&topic, "topic", "t", "", "Name of topic for the action to be performed on")
	cmd.Flags().Int32VarP(&partition, "partition", "p", -1, "Partition ID to be transferred")
	cmd.Flags().Int32VarP(&brokerId, "broker", "b", -1, "Broker ID for the partition to be transferred to")

	return cmd
}
