def dump_catapult_trace(self, path, task_info, breakdowns=False):
        """Dump task profiling information to a file.

        This information can be viewed as a timeline of profiling information
        by going to chrome://tracing in the chrome web browser and loading the
        appropriate file.

        Args:
            path: The filepath to dump the profiling information to.
            task_info: The task info to use to generate the trace.
            breakdowns: Boolean indicating whether to break down the tasks into
               more fine-grained segments.
        """

        workers = self.workers()
        start_time = None
        for info in task_info.values():
            task_start = min(self._get_times(info))
            if not start_time or task_start < start_time:
                start_time = task_start

        def micros(ts):
            return int(1e6 * ts)

        def micros_rel(ts):
            return micros(ts - start_time)

        task_table = ray.global_state.task_table()
        full_trace = []
        workers = self.workers()
        tasks = self.task_table()
        for task_id, info in task_info.items():
          parent_info = task_info.get(tasks[task_id]["TaskSpec"]["ParentTaskID"])
          times = self._get_times(info)
          worker = workers[info["worker_id"]]

          if parent_info:
            parent_worker = workers[parent_info["worker_id"]]
            parent_times = self._get_times(parent_info)
            parent_trace = {
                "cat": "submit_task",
                "pid": "Node " + str(parent_worker["node_ip_address"]),
                "tid": parent_info["worker_id"],
                "ts": micros(min(parent_times)),
                "ph": "s",
                "name": "SubmitTask",
                "args": {},
                "id": str(worker)
            }
            full_trace.append(parent_trace)

            parent = {
                "cat": "submit_task",
                "pid": "Node " + str(parent_worker["node_ip_address"]),
                "tid": parent_info["worker_id"],
                "ts": micros(min(parent_times)),
                "ph": "s",
                "name": "SubmitTask",
                "args": {},
                "id": str(worker)
            }
            full_trace.append(parent)

          task_trace = {
              "cat": "submit_task",
              "pid": "Node " + str(worker["node_ip_address"]),
              "tid": info["worker_id"],
              "ts": micros(min(times)),
              "ph": "f",
              "name": "SubmitTask",
              "args": {},
              "id": str(worker)
          }
          full_trace.append(task_trace)

          task = {
              "name": info["function_name"],
              "cat": "ray_task",
              "ph": "X",
              "ts": micros(min(times)),
              "dur": micros(max(times)) - micros(min(times)),
              "pid": "Node " + str(worker["node_ip_address"]),
              "tid": info["worker_id"],
              "args": info
          }
          full_trace.append(task)

          for obj in task_table[task_id]["TaskSpec"]["ReturnObjectIDs"]:


        with open(path, "w") as outfile:
          json.dump(full_trace, outfile)
        task_info