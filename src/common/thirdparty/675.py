def task_profiles(self, start=None, end=None, num=None):
    """Fetch and return a list of task profiles.

    Args:
      start: The start point of the time window that is queried for tasks.
      end: The end point in time of the time window that is queried for tasks.
      num: A limit on the number of tasks that task_profiles will return.

    Returns:
      A tuple of two elements. The first element is a dictionary mapping the
        task ID of a task to a list of the profiling information for all of the
        executions of that task. The second element is a list of profiling
        information for tasks where the events have no task ID.
    """
    if start is None:
      start = 0
    if num is None:
      num = sys.maxsize

    task_info = dict()
    event_log_sets = self.redis_client.keys("event_log*")

    # The heap is used to maintain the set of x tasks that occurred the most
    # recently across all of the workers, where x is defined as the function
    # parameter num. The key is the start time of the "get_task" component of
    # each task. Calling heappop will result in the taks with the earliest
    # "get_task_start" to be removed from the heap.

    heap = []
    heapq.heapify(heap)
    heap_size = 0
    # Parse through event logs to determine task start and end points.
    for i in range(len(event_log_sets)):
      event_list = self.redis_client.zrangebyscore(event_log_sets[i],
                                                   min=start,
                                                   max=end,
                                                   start=start,
                                                   num=num)
      for event in event_list:
        event_dict = json.loads(event)
        task_id = ""
        for event in event_dict:
          if "task_id" in event[3]:
            task_id = event[3]["task_id"]
        task_info[task_id] = dict()
        for event in event_dict:
          if event[1] == "ray:get_task" and event[2] == 1:
            task_info[task_id]["get_task_start"] = event[0]
            # Add task to min heap by its start point.
            heapq.heappush(heap,
                           (task_info[task_id]["get_task_start"], task_id))
            heap_size += 1
          if event[1] == "ray:get_task" and event[2] == 2:
            task_info[task_id]["get_task_end"] = event[0]
          if event[1] == "ray:import_remote_function" and event[2] == 1:
            task_info[task_id]["import_remote_start"] = event[0]
          if event[1] == "ray:import_remote_function" and event[2] == 2:
            task_info[task_id]["import_remote_end"] = event[0]
          if event[1] == "ray:acquire_lock" and event[2] == 1:
            task_info[task_id]["acquire_lock_start"] = event[0]
          if event[1] == "ray:acquire_lock" and event[2] == 2:
            task_info[task_id]["acquire_lock_end"] = event[0]
          if event[1] == "ray:task:get_arguments" and event[2] == 1:
            task_info[task_id]["get_arguments_start"] = event[0]
          if event[1] == "ray:task:get_arguments" and event[2] == 2:
            task_info[task_id]["get_arguments_end"] = event[0]
          if event[1] == "ray:task:execute" and event[2] == 1:
            task_info[task_id]["execute_start"] = event[0]
          if event[1] == "ray:task:execute" and event[2] == 2:
            task_info[task_id]["execute_end"] = event[0]
          if event[1] == "ray:task:store_outputs" and event[2] == 1:
            task_info[task_id]["store_outputs_start"] = event[0]
          if event[1] == "ray:task:store_outputs" and event[2] == 2:
            task_info[task_id]["store_outputs_end"] = event[0]
          if "worker_id" in event[3]:
            task_info[task_id]["worker_id"] = event[3]["worker_id"]
          if "function_name" in event[3]:
            task_info[task_id]["function_name"] = event[3]["function_name"]
        if heap_size > num:
          min_task, task_id_hex = heapq.heappop(heap)
          del task_info[task_id_hex]
          heap_size -= 1
    return task_info


    def task_profiles(self, start=None, end=None, num=None):
    """Fetch and return a list of task profiles.

    Returns:
      A tuple of two elements. The first element is a dictionary mapping the
        task ID of a task to a list of the profiling information for all of the
        executions of that task. The second element is a list of profiling
        information for tasks where the events have no task ID.
    """
    if start is None:
      start = 0
    if num is None:
      num = sys.maxsize
    task_info = dict()
    event_log_sets = self.redis_client.keys("event_log*")
    '''
    The heap is used to maintain the set of x tasks that occurred the most
    recently across all of the workers, where x is defined as the function
    parameter num. The key is the start time of the "get_task" component of
    each task. Calling heappop will result in the taks with the earliest
    "get_task_start" to be removed from the heap.
    '''
    heap = []
    heapq.heapify(heap)
    heap_size = 0
    # Parse through event logs to determine task start and end points.
    for i in range(len(event_log_sets)):
      event_list = self.redis_client.zrangebyscore(event_log_sets[i], min=start,
                                                   max=end, start=0, num=num)
      for event in event_list:
        event_dict = ujson.loads(event)
        task_id = ""
        for event in event_dict:
          if "task_id" in event[3]:
            task_id = event[3]["task_id"]
        task_info[task_id] = dict()
        for event in event_dict:
          if event[1] == "ray:get_task" and event[2] == 1:
            task_info[task_id]["get_task_start"] = event[0]
            # Add task to min heap by its start point.
            heapq.heappush(heap, (task_info[task_id]["get_task_start"], task_id))
            heap_size += 1
          if event[1] == "ray:get_task" and event[2] == 2:
            task_info[task_id]["get_task_end"] = event[0]
          if event[1] == "ray:import_remote_function" and event[2] == 1:
            task_info[task_id]["import_remote_start"] = event[0]
          if event[1] == "ray:import_remote_function" and event[2] == 2:
            task_info[task_id]["import_remote_end"] = event[0]
          if event[1] == "ray:acquire_lock" and event[2] == 1:
            task_info[task_id]["acquire_lock_start"] = event[0]
          if event[1] == "ray:acquire_lock" and event[2] == 2:
            task_info[task_id]["acquire_lock_end"] = event[0]
          if event[1] == "ray:task:get_arguments" and event[2] == 1:
            task_info[task_id]["get_arguments_start"] = event[0]
          if event[1] == "ray:task:get_arguments" and event[2] == 2:
            task_info[task_id]["get_arguments_end"] = event[0]
          if event[1] == "ray:task:execute" and event[2] == 1:
            task_info[task_id]["execute_start"] = event[0]
          if event[1] == "ray:task:execute" and event[2] == 2:
            task_info[task_id]["execute_end"] = event[0]
          if event[1] == "ray:task:store_outputs" and event[2] == 1:
            task_info[task_id]["store_outputs_start"] = event[0]
          if event[1] == "ray:task:store_outputs" and event[2] == 2:
            task_info[task_id]["store_outputs_end"] = event[0]
          if "worker_id" in event[3]:
            task_info[task_id]["worker_id"] = event[3]["worker_id"]
          if "function_name" in event[3]:
            task_info[task_id]["function_name"] = event[3]["function_name"]
        if heap_size > num:
          min_task, taskid = heapq.heappop(heap)
          del task_info[taskid]
          heap_size -= 1
    return task_info