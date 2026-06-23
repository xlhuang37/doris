// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <brpc/closure_guard.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/types.pb.h>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "exec/pipeline/pipeline.h"
#include "exec/pipeline/pipeline_task.h"
#include "runtime/query_context.h"
#include "runtime/runtime_profile.h"
#include "runtime/runtime_state.h"
#include "runtime/task_execution_context.h"
#include "util/stopwatch.hpp"
#include "util/uid_util.h"

namespace doris {
struct ReportStatusRequest;
class ExecEnv;
class RuntimeFilterMergeControllerEntity;
class TDataSink;
class TPipelineFragmentParams;

class Dependency;

class PipelineFragmentContext : public TaskExecutionContext {
public:
    ENABLE_FACTORY_CREATOR(PipelineFragmentContext);
    PipelineFragmentContext(TUniqueId query_id, const TPipelineFragmentParams& request,
                            std::shared_ptr<QueryContext> query_ctx, ExecEnv* exec_env,
                            const std::function<void(RuntimeState*, Status*)>& call_back);

    ~PipelineFragmentContext() override;

    void print_profile(const std::string& extra_info);

    std::vector<std::shared_ptr<TRuntimeProfileTree>> collect_realtime_profile() const;
    std::shared_ptr<TRuntimeProfileTree> collect_realtime_load_channel_profile() const;

    bool is_timeout(timespec now) const;

    uint64_t elapsed_time() const { return _fragment_watcher.elapsed_time(); }

    int timeout_second() const { return _timeout; }

    PipelinePtr add_pipeline(PipelinePtr parent = nullptr, int idx = -1);

    QueryContext* get_query_ctx() { return _query_ctx.get(); }
    [[nodiscard]] bool is_canceled() const { return _query_ctx->is_cancelled(); }

    Status prepare(ThreadPool* thread_pool);

    Status submit();

    void set_is_report_success(bool is_report_success) { _is_report_success = is_report_success; }

    void cancel(const Status reason);

    bool notify_close();

    TUniqueId get_query_id() const { return _query_id; }

    [[nodiscard]] int get_fragment_id() const { return _fragment_id; }

    // Global CPU runtime accumulated across all instances and pipeline tasks of
    // this fragment. It drives the fragment-granular MLFQ demotion in the
    // pipeline task scheduler: all of a fragment's tasks share this single
    // counter, so a fragment is demoted as a whole based on its total CPU usage.
    void add_fragment_runtime_ns(uint64_t delta) {
        _fragment_runtime_ns.fetch_add(delta, std::memory_order_relaxed);
    }
    uint64_t fragment_runtime_ns() const {
        return _fragment_runtime_ns.load(std::memory_order_relaxed);
    }
    std::atomic<uint64_t>* fragment_runtime_counter() { return &_fragment_runtime_ns; }

    // Inelastic-first scheduling. A fragment is "inelastic" when it has too few
    // runnable (non-blocked) pipeline tasks to use much parallelism; such a
    // fragment's tasks are boosted to the top scheduler priority level so it can
    // finish quickly and release resources. The runnable count is maintained from
    // PipelineTask::_state_transition (the sole place tasks enter/leave RUNNABLE).
    void adjust_runnable_pipelines(int delta) {
        int count = _runnable_pipeline_count.fetch_add(delta, std::memory_order_relaxed) + delta;
        _is_inelastic.store(count <= _inelastic_threshold, std::memory_order_relaxed);
    }
    bool is_inelastic() const { return _is_inelastic.load(std::memory_order_relaxed); }

    void decrement_running_task(PipelineId pipeline_id);

    uint32_t rec_cte_stage() const { return _rec_cte_stage; }
    void set_rec_cte_stage(uint32_t stage) { _rec_cte_stage = stage; }

    Status send_report(bool);

    void trigger_report_if_necessary();
    void refresh_next_report_time();

    std::string debug_string();

    [[nodiscard]] int next_operator_id() { return _operator_id--; }

    [[nodiscard]] int max_operator_id() const { return _operator_id; }

    [[nodiscard]] int next_sink_operator_id() { return _sink_operator_id--; }

    [[nodiscard]] size_t get_revocable_size(bool* has_running_task) const;

    [[nodiscard]] std::vector<PipelineTask*> get_revocable_tasks() const;

    void clear_finished_tasks() {
        if (_need_notify_close) {
            return;
        }
        for (size_t j = 0; j < _tasks.size(); j++) {
            for (size_t i = 0; i < _tasks[j].size(); i++) {
                _tasks[j][i].first->stop_if_finished();
            }
        }
    }

    std::string get_load_error_url();
    std::string get_first_error_msg();

    std::set<int> get_deregister_runtime_filter() const;

    // Store the brpc ClosureGuard so the RPC response is deferred until this PFC is destroyed.
    // When need_send_report_on_destruction is true (final_close), send the report immediately
    // and do not store the guard (let it fire on return to complete the RPC).
    //
    // Thread safety: This method is NOT thread-safe. It reads/writes _wait_close_guard without
    // synchronization. Currently it is only called from rerun_fragment() which is invoked
    // sequentially by RecCTESourceOperatorX (a serial operator) — one opcode at a time per
    // fragment. Do NOT call this concurrently from multiple threads.
    Status listen_wait_close(const std::shared_ptr<brpc::ClosureGuard>& guard,
                             bool need_send_report_on_destruction) {
        if (_wait_close_guard) {
            return Status::InternalError("Already listening wait close");
        }
        if (need_send_report_on_destruction) {
            return send_report(true);
        } else {
            _wait_close_guard = guard;
        }
        return Status::OK();
    }

private:
    void _coordinator_callback(const ReportStatusRequest& req);
    std::string _to_http_path(const std::string& file_name) const;

    void _release_resource();

    Status _build_and_prepare_full_pipeline(ThreadPool* thread_pool);

    Status _build_pipelines(ObjectPool* pool, const DescriptorTbl& descs, OperatorPtr* root,
                            PipelinePtr cur_pipe);
    Status _create_tree_helper(ObjectPool* pool, const std::vector<TPlanNode>& tnodes,
                               const DescriptorTbl& descs, OperatorPtr parent, int* node_idx,
                               OperatorPtr* root, PipelinePtr& cur_pipe, int child_idx,
                               const bool followed_by_shuffled_join,
                               const bool require_bucket_distribution);

    Status _create_operator(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs,
                            OperatorPtr& op, PipelinePtr& cur_pipe, int parent_idx, int child_idx,
                            const bool followed_by_shuffled_join,
                            const bool require_bucket_distribution, OperatorPtr& cache_op);
    template <bool is_intersect>
    Status _build_operators_for_set_operation_node(ObjectPool* pool, const TPlanNode& tnode,
                                                   const DescriptorTbl& descs, OperatorPtr& op,
                                                   PipelinePtr& cur_pipe,
                                                   std::vector<DataSinkOperatorPtr>& sink_ops);

    Status _create_data_sink(ObjectPool* pool, const TDataSink& thrift_sink,
                             const std::vector<TExpr>& output_exprs,
                             const TPipelineFragmentParams& params, const RowDescriptor& row_desc,
                             RuntimeState* state, DescriptorTbl& desc_tbl,
                             PipelineId cur_pipeline_id);
    Status _plan_local_exchange(int num_buckets,
                                const std::map<int, int>& bucket_seq_to_instance_idx,
                                const std::map<int, int>& shuffle_idx_to_instance_idx);
    Status _plan_local_exchange(int num_buckets, int pip_idx, PipelinePtr pip,
                                const std::map<int, int>& bucket_seq_to_instance_idx,
                                const std::map<int, int>& shuffle_idx_to_instance_idx);
    void _inherit_pipeline_properties(const DataDistribution& data_distribution,
                                      PipelinePtr pipe_with_source, PipelinePtr pipe_with_sink);
    Status _add_local_exchange(int pip_idx, int idx, int node_id, ObjectPool* pool,
                               PipelinePtr cur_pipe, DataDistribution data_distribution,
                               bool* do_local_exchange, int num_buckets,
                               const std::map<int, int>& bucket_seq_to_instance_idx,
                               const std::map<int, int>& shuffle_idx_to_instance_idx);
    Status _add_local_exchange_impl(int idx, ObjectPool* pool, PipelinePtr cur_pipe,
                                    PipelinePtr new_pip, DataDistribution data_distribution,
                                    bool* do_local_exchange, int num_buckets,
                                    const std::map<int, int>& bucket_seq_to_instance_idx,
                                    const std::map<int, int>& shuffle_idx_to_instance_idx);

    Status _build_pipeline_tasks(ThreadPool* thread_pool);
    Status _build_pipeline_tasks_for_instance(
            int instance_idx,
            const std::vector<std::shared_ptr<RuntimeProfile>>& pipeline_id_to_profile);
    // Close the fragment instance and return true if the caller should call
    // remove_pipeline_context() **after** releasing _task_mutex. This avoids
    // holding _task_mutex while acquiring _pipeline_map's shard lock, which
    // would create an ABBA deadlock with dump_pipeline_tasks().
    bool _close_fragment_instance();
    void _init_next_report_time();

    // Id of this query
    TUniqueId _query_id;
    int _fragment_id;

    // Global CPU runtime counter for this logical fragment. Shared by all of the
    // fragment's pipeline tasks (across instances and across the blocking/simple
    // sub-schedulers) and used by the pipeline MLFQ to compute the priority level.
    std::atomic<uint64_t> _fragment_runtime_ns {0};

    // Inelastic-first scheduling state for this logical fragment.
    // `_runnable_pipeline_count` tracks how many of the fragment's pipeline tasks are
    // currently in RUNNABLE state (ready or executing, excluding blocked). When it is
    // at or below `_inelastic_threshold`, `_is_inelastic` is set and the scheduler
    // routes this fragment's tasks to the top priority level.
    std::atomic<int> _runnable_pipeline_count {0};
    std::atomic<bool> _is_inelastic {false};
    const int _inelastic_threshold = config::pipeline_inelastic_runnable_threshold;

    ExecEnv* _exec_env = nullptr;

    std::atomic_bool _prepared = false;
    bool _submitted = false;

    Pipelines _pipelines;
    PipelineId _next_pipeline_id = 0;
    std::mutex _task_mutex;
    int _closed_tasks = 0;
    // After prepared, `_total_tasks` is equal to the size of `_tasks`.
    // When submit fail, `_total_tasks` is equal to the number of tasks submitted.
    std::atomic<int> _total_tasks = 0;

    std::unique_ptr<RuntimeProfile> _fragment_level_profile;
    bool _is_report_success = false;

    std::unique_ptr<RuntimeState> _runtime_state;

    std::shared_ptr<QueryContext> _query_ctx;

    MonotonicStopWatch _fragment_watcher;
    RuntimeProfile::Counter* _prepare_timer = nullptr;
    RuntimeProfile::Counter* _init_context_timer = nullptr;
    RuntimeProfile::Counter* _build_pipelines_timer = nullptr;
    RuntimeProfile::Counter* _plan_local_exchanger_timer = nullptr;
    RuntimeProfile::Counter* _prepare_all_pipelines_timer = nullptr;
    RuntimeProfile::Counter* _build_tasks_timer = nullptr;

    std::function<void(RuntimeState*, Status*)> _call_back;
    std::atomic_bool _is_fragment_instance_closed = false;

    // If this is set to false, and '_is_report_success' is false as well,
    // This executor will not report status to FE on being cancelled.
    bool _is_report_on_cancel;

    // 0 indicates reporting is in progress or not required
    std::atomic_bool _disable_period_report = true;
    std::atomic_uint64_t _previous_report_time = 0;

    DescriptorTbl* _desc_tbl = nullptr;
    int _num_instances = 1;

    int _timeout = -1;
    bool _use_serial_source = false;

    OperatorPtr _root_op = nullptr;
    //
    /**
     * Matrix stores tasks with local runtime states.
     * This is a [n * m] matrix. n is parallelism of pipeline engine and m is the number of pipelines.
     *
     * 2-D matrix:
     * +-------------------------+------------+-------+
     * |            | Pipeline 0 | Pipeline 1 |  ...  |
     * +------------+------------+------------+-------+
     * | Instance 0 |  task 0-0  |  task 0-1  |  ...  |
     * +------------+------------+------------+-------+
     * | Instance 1 |  task 1-0  |  task 1-1  |  ...  |
     * +------------+------------+------------+-------+
     * | ...                                          |
     * +--------------------------------------+-------+
     */
    std::vector<
            std::vector<std::pair<std::shared_ptr<PipelineTask>, std::unique_ptr<RuntimeState>>>>
            _tasks;

    // TODO: remove the _sink and _multi_cast_stream_sink_senders to set both
    // of it in pipeline task not the fragment_context
#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wshadow-field"
#endif
    DataSinkOperatorPtr _sink = nullptr;
#ifdef __clang__
#pragma clang diagnostic pop
#endif

    // `_dag` manage dependencies between pipelines by pipeline ID. the indices will be blocked by members
    std::map<PipelineId, std::vector<PipelineId>> _dag;

    // We use preorder traversal to create an operator tree. When we meet a join node, we should
    // build probe operator and build operator in separate pipelines. To do this, we should build
    // ProbeSide first, and use `_pipelines_to_build` to store which pipeline the build operator
    // is in, so we can build BuildSide once we complete probe side.
    struct pipeline_parent_map {
        std::map<int, std::vector<PipelinePtr>> _build_side_pipelines;
        void push(int parent_node_id, PipelinePtr pipeline) {
            if (!_build_side_pipelines.contains(parent_node_id)) {
                _build_side_pipelines.insert({parent_node_id, {pipeline}});
            } else {
                _build_side_pipelines[parent_node_id].push_back(pipeline);
            }
        }
        void pop(PipelinePtr& cur_pipe, int parent_node_id, int child_idx) {
            if (!_build_side_pipelines.contains(parent_node_id)) {
                return;
            }
            DCHECK(_build_side_pipelines.contains(parent_node_id));
            auto& child_pipeline = _build_side_pipelines[parent_node_id];
            DCHECK(child_idx < child_pipeline.size());
            cur_pipe = child_pipeline[child_idx];
        }
        void clear() { _build_side_pipelines.clear(); }
    } _pipeline_parent_map;

    std::mutex _state_map_lock;

    int _operator_id = 0;
    int _sink_operator_id = 0;
    /**
     * Some states are shared by tasks in different pipeline task (e.g. local exchange , broadcast join).
     *
     * local exchange sink 0 ->                               -> local exchange source 0
     *                            LocalExchangeSharedState
     * local exchange sink 1 ->                               -> local exchange source 1
     *
     * hash join build sink 0 ->                               -> hash join build source 0
     *                              HashJoinSharedState
     * hash join build sink 1 ->                               -> hash join build source 1
     *
     * So we should keep states here.
     */
    std::map<int,
             std::pair<std::shared_ptr<BasicSharedState>, std::vector<std::shared_ptr<Dependency>>>>
            _op_id_to_shared_state;

    std::map<PipelineId, Pipeline*> _pip_id_to_pipeline;
    std::vector<std::unique_ptr<RuntimeFilterMgr>> _runtime_filter_mgr_map;

    //Here are two types of runtime states:
    //    - _runtime state is at the Fragment level.
    //    - _task_runtime_states is at the task level, unique to each task.

    std::vector<TUniqueId> _fragment_instance_ids;

    // Total instance num running on all BEs
    int _total_instances = -1;

    TPipelineFragmentParams _params;
    int32_t _parallel_instances = 0;

    std::atomic<bool> _need_notify_close = false;
    // Holds the brpc ClosureGuard for async wait-close during recursive CTE rerun.
    // When the PFC finishes closing and is destroyed, the shared_ptr destructor fires
    // the ClosureGuard, which completes the brpc response to the RecCTESourceOperatorX.
    // Only written by listen_wait_close() from a single rerun_fragment RPC thread.
    std::shared_ptr<brpc::ClosureGuard> _wait_close_guard = nullptr;

    // The recursion round number for recursive CTE fragments.
    // Incremented each time the fragment is rebuilt via rerun_fragment(rebuild).
    // Used to stamp runtime filter RPCs so stale messages from old rounds are discarded.
    uint32_t _rec_cte_stage = 0;
};
} // namespace doris
