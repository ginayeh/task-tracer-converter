#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import argparse
import json
from sets import Set
import sqlite3
from collections import namedtuple


tasks = {}
show_warnings = False
processes = {}
threads = {}


class ParseError(Exception):
  def __init__(self, error_msg):
    self.msg = error_msg
    self.log = ''

class BaseObject(object):
  def __init__(self, id, name=''):
    self.id = id
    self.name = name

  def pretty_dict(self):
    return {key: value for key, value in self.__dict__.iteritems() if not key.startswith('_')}

class Process(BaseObject):
  def __init__(self, id, name):
    super(Process, self).__init__(id, name)
    self._mem_offset = 0

class Thread(BaseObject):
  def __init__(self, id, name):
    super(Thread, self).__init__(id, name)

Label = namedtuple('Label', 'timestamp label')

class Task(BaseObject):
  def __init__(self, id):
    super(Task, self).__init__(id)

    self.sourceEventId = 0
    self.sourceEventType = None
    self.processId = 0
    self.threadId = 0
    self.parentTaskId = 0
    self.labels = []
    self._vptr = 0

    # Timestamp information
    self.dispatch = 0
    self.begin = 0
    self.end = 0
    self.latency = 0
    self.executionTime = 0
    self._userCpuTime = 0
    self._sysCpuTime = 0

  def add_label(self, timestamp, label):
    self.labels.append(Label(timestamp, label))

def find_char_and_split(string, char=' ', num_split=-1):
  """
    Find the delimiter first and then split the string with the delimiter.

    returns:
      A list of the words in the string after spliting the string.
      None when failed to find the delimiter in the string.
  """
  if string.find(char) == -1:
    raise ParseError('Extract error: no \'{}\''.format(char))

  if string.count(char) < num_split:
    raise ParseError('Extract error: not enough \'{}\'s'.format(char))


  if num_split == -1:
    return string.split(char)
  else:
    return string.split(char, num_split)

def extract_info(log):
  """
    Discard redundant message and extract information.

    log: Raw log.
      Ex. I/TaskTracer( 1570): 1 6743098656705 316475166353175 6743098656704 1 6743098656704

    returns:
      A list is returned after successfully extract information from input.
      None is returned if the input is redundant.
      False is returned  when an error occurred.
  """
  raw_log = log
  try:
    # Remove tag name. Ex. 'I/TaskTracer( 1570):'
    tokens = find_char_and_split(log, ':', 1)
    log = tokens[1].strip()

    # Retrieve log type
    tokens = find_char_and_split(log, ' ', 1)
    log_type = int(tokens[0])
    log = tokens[1].strip()

    # log_type:
    #   0 - DISPATCH. Ex. 0 taskId dispatch sourceEventId sourceEventType parentTaskId
    #   1 - BEGIN.    Ex. 1 taskId begin userCpuTime sysCpuTime processId "processName" threadId "threadName"
    #   2 - END.      Ex. 2 taskId end userCpuTime sysCpuTime
    #   3 - LABEL.    Ex. 3 taskId timestamp "label"
    #   4 - VPTR.     Ex. 4 address
    info = None

    if log_type == 3:
      tokens = find_char_and_split(log, ' ', 2)
      info = [log_type] + tokens[0:2]

      tokens = find_char_and_split(tokens[2], '"', 2)
      info.append(tokens[1])
    elif log_type == 1:
      tokens = find_char_and_split(log, ' ', 5)
      info = [log_type] + tokens[0:5]

      tokens = find_char_and_split(tokens[5], '"', 4)
      info.append(tokens[1])
      info.append(tokens[2].strip())
      info.append(tokens[3])
    else:
      tokens = find_char_and_split(log)
      info = [log_type] + tokens
  except ParseError:
    raise

  return info

def verify_info(info):
  """
    Verify information value and format.

    info: A list of information.

    returns:
      True when verify passed.
      False when verify failed.
  """
  log_type = int(info[0])
  if not log_type in range(0, 5):
    raise ParseError('Verify error: invalid log type \'{}\''.format(log_type))

  if any(((log_type == 0) and (len(info) != 6),
          (log_type == 1) and (len(info) != 9),
          (log_type == 2) and (len(info) != 5),
          (log_type == 3) and (len(info) != 4))):
     raise ParseError('Verify error: incomplete information')

def set_task_info(info):
  """
    Set task properties based on log_type.

    info: A list of information. Each type has its own format.
      DISPATCH: [0 taskId dispatch sourceEventId sourceEventType parentTaskId]
      BEGIN:    [1 taskId begin userCpuTime sysCpuTime processId processName threadId threadName]
      END:      [2 taskId end userCpuTime sysCpuTime]
      LABEL:    [3 taskId timestamp, label]
      VPTR:     [4 taskId vptr]
  """
  log_type = int(info[0])

  task_id = info[1]
  if task_id not in tasks:
    if log_type == 0:
      tasks[task_id] = Task(int(task_id))
    else:
      if show_warnings:
        print 'Skip task {} because of incomplete logs.'.format(task_id)
      return

  if log_type == 4:
    tasks[task_id]._vptr = int(info[2], 16)
    return

  timestamp = int(info[2])
  if log_type == 0:
    tasks[task_id].dispatch = timestamp
    tasks[task_id].sourceEventId = int(info[3])
    tasks[task_id].sourceEventType = int(info[4])
    tasks[task_id].parentTaskId = int(info[5])
  elif log_type == 1:
    tasks[task_id].begin = timestamp
    tasks[task_id].latency = tasks[task_id].begin - tasks[task_id].dispatch
    tasks[task_id]._userCpuTime = int(info[3])
    tasks[task_id]._sysCpuTime = int(info[4])

    process_id = int(info[5])
    tasks[task_id].processId = process_id
    if process_id not in processes:
      processes[process_id] = Process(process_id, info[6])
    elif all((info[6] != processes[process_id].name,
              info[6] != '(Preallocated app)')):
      processes[process_id].name = info[6]

    thread_id = int(info[7])
    tasks[task_id].threadId = thread_id
    if thread_id not in threads:
      threads[thread_id] = Thread(thread_id, info[8])
    else:
      threads[thread_id].name = info[8]
  elif log_type == 2:
    tasks[task_id].end = timestamp
    tasks[task_id].executionTime = tasks[task_id].end - tasks[task_id].begin
    tasks[task_id]._userCpuTime -= int(info[3])
    tasks[task_id]._sysCpuTime -= int(info[4])

    if tasks[task_id]._userCpuTime != 0:
      cpuTime = -tasks[task_id]._userCpuTime / float(tasks[task_id].executionTime) * 100.0
      tasks[task_id].add_label(timestamp, 'UserCpuTime:{}'.format(round(cpuTime, 2)))

    if tasks[task_id]._sysCpuTime != 0:
      cpuTime = -tasks[task_id]._sysCpuTime / float(tasks[task_id].executionTime) * 100.0
      tasks[task_id].add_label(timestamp, 'SysCpuTime: {}'.format(round(cpuTime, 2)))
  elif log_type == 3:
    tasks[task_id].add_label(timestamp, info[3])

def parse_log(input_name):
  """
    Parse input file line by line.

    input_name: Input filename.

    returns:
      True if the whole file is parsed successfully.
      False when error occurred.
  """
  with open(input_name, 'r') as log_file:
    all_log = log_file.readlines()

  for line in all_log:
    # Remove dummy log like '----------- beginning of...'
    if line.startswith('-'):
      continue

    try:
      info = extract_info(line.strip())
      verify_info(info)
    except ParseError as error:
      error.log(line.strip())
      raise

    set_task_info(info)

def retrieve_begin_end_time():
  """Scan through all timestamps and return the min and the max."""
  all_timestamps = Set([])
  for task_id, task_object in tasks.iteritems():
    all_timestamps.add(task_object.dispatch)
    all_timestamps.add(task_object.begin)
    all_timestamps.add(task_object.end)

  # The initial value for these timestamps is 0, so we have to remove it.
  if 0 in all_timestamps:
    all_timestamps.remove(0)

  return [min(all_timestamps), max(all_timestamps)]

def replace_undefined_timestamp(end_time):
  """Replace undefined timestamp with the max of all timestamps."""
  for task_id, task_object in tasks.iteritems():
    if task_object.begin == 0:
      task_object.begin = end_time
    if task_object.end == 0:
      task_object.end = end_time

def output_json(output_name, begin_time, end_time):
  """
    Write tasks out in JSON format.

    output_name: Output filename.
    begin_time: the min of all timestamps.
    end_time: the max of all timestamps.
  """
  output_file = open(output_name, 'w')
  output_file.write('{\"begin\": %d, \"end\": %d, \"processes\": '
                    % (begin_time, end_time))
  output_file.write(json.dumps(processes.values(), default=lambda o:o.pretty_dict(),
                    indent=4))
  output_file.write(', \"threads\": ')
  output_file.write(json.dumps(threads.values(), default=lambda o:o.pretty_dict(),
                    indent=4))
  output_file.write(', \"tasks\": ')
  output_file.write(json.dumps(tasks.values(), default=lambda o: o.pretty_dict(),
                               indent=4))

  output_file.write('}')
  output_file.close()

def get_arguments(argv):
  """Parse arguments."""
  parser = argparse.ArgumentParser()
  parser.add_argument('-i', '--input-file', help='Input file', required=True)
  parser.add_argument('-o', '--output-file', help='Output file (Optional)',
                      default='task_tracer_data.json')
  parser.add_argument('-c', '--check-parent-task-id', action='store_const',
                      const=True, help='Check parentTaskId (if possible)')
  parser.add_argument('-w', '--show-warnings', action='store_const', const=True,
                      help='Show warnings')
  parser.add_argument('-p', '--print-all-tasks', action='store_const',
                      const=True, help='Print all tasks')
  return parser.parse_args()

def create_table_and_insert_tasks():
  """Create a database and insert all tasks into a table."""
  print 'Create database \'task_tracer.db\'.'
  conn = sqlite3.connect('task_tracer.db')
  with conn:
    cur = conn.cursor()

    # Delete the table if exists and re-create the table
    cur.execute('DROP TABLE IF EXISTS Tasks')
    cur.execute(('CREATE TABLE Tasks('
                 'taskId INT, threadId INT, begin INT, end INT)'))

    # Insert information into table
    for task_id, task_obj in tasks.iteritems():
      # Only tasks which includes complete information are inserted into database
      if any((task_obj.threadId is None,
              task_obj.begin is 0,
              task_obj.end is 0)):
        continue

      insert_cmd = ('INSERT INTO Tasks VALUES({}, {}, {}, {})'.format(task_id,
                    task_obj.threadId, task_obj.begin, task_obj.end))
      cur.execute(insert_cmd)

    cur.execute('SELECT * FROM Tasks')
    num_records = len(cur.fetchall())
    print 'Insert {} records into Table Tasks'.format(num_records)

def check_parent_task_id():
  """
    Prepare a database first, and verify the value of parentTaskId.

    A taskId consist of its threadId and a serial number.
    |    threadId     |   serial num.   |
    | <-- 32 bits --> | <-- 32 bits --> |

    Additionally, the child task is dispatched during the execution time of its
    parent task. And, for each thread, tasks should be executed once at a time.
    Thus, only ONE task should be returned when we query the database with the
    following  conditions:
      1. parent.threadId = high_bits(child.taskId)
      2. parent.begin < child.dispatch
      3. parent.end > child.dispatch
  """
  create_table_and_insert_tasks()

  conn = sqlite3.connect('task_tracer.db')
  num_no_result = 0
  num_multi_results = 0
  num_error_result = 0

  with conn:
    cur = conn.cursor()

    # Verify parentTaskId with query results
    for task_id, task_obj in tasks.iteritems():
      if task_obj.dispatch is 0:
        continue

      # Retrieve the 32 high-order bits
      thread_id = int(task_id) >> 32
      select_cmd = ('SELECT taskId FROM Tasks ' +
                    'WHERE threadId={} AND '.format(thread_id) +
                    'begin<={} AND '.format(task_obj.dispatch) +
                    'end>={}'.format(task_obj.dispatch))
      cur.execute(select_cmd)

      rows = cur.fetchall()
      if len(rows) is 0:
        num_no_result += 1
        continue
      elif len(rows) > 1:
        num_multi_results += 1
        continue
      elif task_obj.parentTaskId != rows[0][0]:
        print ("Verify error: inconsistent 'parentTaskId'. Input: " +
               '{}, Query result: {}'.format(task_obj.parentTaskId, rows[0][0]))
        num_error_result += 1
        continue

  num_total_tasks = float(len(tasks))
  num_verified_task = len(tasks) - num_no_result - \
                      num_multi_results - num_error_result
  print ('{} tasks ({}%) failed to query parentTaskId in database.\n'.
         format(num_no_result, float(num_no_result)/num_total_tasks) +
         '{} tasks ({}%) got multiple results for parentTaskId in database.\n'.
         format(num_multi_results, float(num_multi_results)/num_total_tasks) +
         '{} tasks ({}%) got inconsistent parentTaskId in database.\n'.
         format(num_error_result, float(num_error_result)/num_total_tasks) +
         '{} tasks ({}%) are verified.'.
         format(num_verified_task, float(num_verified_task)/num_total_tasks))

def print_all_tasks():
  """Print task information in text mode."""
  for task_id, task_obj in tasks.iteritems():
    labels_str = json.dumps(task_obj.labels, default=lambda o: o.__dict__)
    print ('taskId: {}, '.format(task_id) +
           'sourceEventType: {}, '.format(task_obj.sourceEventType) +
           'sourceEventId: {}, '.format(task_obj.sourceEventId) +
           'processId: {}, '.format(task_obj.processId) +
           'threadID: {}, '.format(task_obj.threadId) +
           'parentTaskId: {}, '.format(task_obj.parentTaskId) +
           'dispatch: {}, '.format(task_obj.dispatch) +
           'begin: {}, '.format(task_obj.begin) +
           'end: {}, '.format(task_obj.end) +
           'labels: {}'.format(labels_str))

def binary_search(address, x, lo=0, hi=None):
  if hi is None:
    hi = len(address)
    while lo < hi:
      if (hi - lo == 1):
        return address[lo][1]

      mid = (lo + hi) / 2
      midval = address[mid][0]
      if (midval < x):
        lo = mid + 1
      elif (midval > x):
        hi = mid
      else:
        return mid

def retrieve_task_name():
  """
    
  """
  # Read file 'mem_offset' generated by prepare-data.sh
  with open('mem_offset', 'r') as mmaps_file:
    all_mem_offset = mmaps_file.readlines()

  for line in all_mem_offset:
    try:
      tokens = find_char_and_split(line)
    except ParseError:
      raise

    process_id = int(tokens[0].strip())
    if process_id not in processes:
      continue

    processes[process_id]._mem_offset = int(tokens[1].strip(), 16)

  # Read file 'symbol' generated by prepare-data.sh
  with open('symbol', 'r') as symbol_file:
    all_symbols = symbol_file.readlines()

  address = []
  for line in all_symbols:
    try:
      tokens = find_char_and_split(line, ' ', 4)
    except ParseError:
      raise

    if len(tokens[0]) == 0:
      continue

    address.append((int(tokens[0], 16), tokens[4].strip()))

  # Get name for each task
  for task_id, task_obj in tasks.iteritems():
    if not (task_obj._vptr and task_obj.processId and
      processes[task_obj.processId]._mem_offset):
      if show_warnings:
        print 'Skip task {} because of incomplete mem_offset.'.format(task_id)
      continue

    offset = task_obj._vptr - processes[task_obj.processId]._mem_offset

    task_obj.name = binary_search(address, offset)

def main(argv=sys.argv[:]):
  args = get_arguments(argv)

  print 'Input:', args.input_file
  print 'Output:', args.output_file

  global show_warnings
  show_warnings = args.show_warnings

  try:
    parse_log(args.input_file)
    print len(tasks), 'tasks has been created successfully.'

    retrieve_task_name()
  except ParseError as error:
    print error.msg
    if error.log:
      print '@line: \'{}\''.format(error.log)
    sys.exit()

  if args.check_parent_task_id:
    check_parent_task_id()

  if len(tasks) == 0:
    sys.exit()

  [begin_time, end_time] = retrieve_begin_end_time();
  replace_undefined_timestamp(end_time);

  output_json(args.output_file, begin_time, end_time)

  print len(tasks), 'tasks has been written to JSON output successfully.'
  if args.print_all_tasks:
    print_all_tasks()

if __name__ == '__main__':
  sys.exit(main())

