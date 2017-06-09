def worker_to_tasks(self):
    event_names = self.redis_client.keys("event_log*")
    results = dict()
    for i in range(len(event_names)):
      event_list = self.redis_client.lrange(event_names[i], 0, -1)
      for event in event_list:
        event_dict = json.loads(event.decode("ascii"))
        worker_id = ""
        task_id = ""
        for element in event_dict:
          if 'task_id' in element[3] and 'worker_id' in element[3]:
            task_id = element[3]['task_id']
            worker_id = element[3]['worker_id']
        if task_id == "" or worker_id == "":
          return "Task_id or worker_id not found. This shouldn't happen."
        else:
          if results[worker_id] == None:
            results[worker_id] = []
          results[worker_id].append(task_id)
    return results