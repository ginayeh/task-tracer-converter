#!/usr/bin/env python
# -*- coding: utf-8 -*-


import sys
import argparse
import json
from sets import Set
import sqlite3
from collections import namedtuple
from os import listdir
from os.path import isfile
import subprocess


tasks = {}
show_warnings = False
processes = {}
threads = {}


class ReadError(Exception):
  def __init__(self, error_msg):
    self.msg = error_msg

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

class Label(object):
  def __init__(self, timestamp, label):
    super(Label, self).__init__()
    self.timestamp = timestamp
    self.label = label

  def pretty_dict(self):
    return self.__dict__

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
    print num_split, string
    raise ParseError('Extract error: no enough \'{}\''.format(char))


  if num_split == -1:
    return string.split(char)
  else:
    return string.split(char, num_split)

def verify_info(info):
  """
    Verify task information based on log type.

    info: A list of task properties.
      DISPATCH: [0 taskId dispatch sourceEventId sourceEventType parentTaskId]
      BEGIN:    [1 taskId begin processId threadId]
      END:      [2 taskId end]
      LABEL:    [3 taskId timestamp, label]
      VPTR:     [4 taskId vptr]

    returns:
      True when verification passed.
      False when verification failed.
  """
  log_type = info[0]
  if not log_type in range(0, 5):
    raise ParseError('Verify error: invalid log type \'{}\''.format(log_type))

  if any(((log_type == 0) and (len(info) != 6),
          (log_type == 1) and (len(info) != 5),
          (log_type == 2) and (len(info) != 3),
          (log_type == 3) and (len(info) != 4),
          (log_type == 4) and (len(info) != 3))):
     raise ParseError('Verify error: incomplete information')

def set_task_info(info, process_id):
  """
    Set task properties based on log type.

    info: A list of verified task properties.
    process_id: Default value of Task.processId.
  """
  log_type = info[0]
  task_id = info[1]
  if task_id not in tasks:
    tasks[task_id] = Task(int(task_id))
    tasks[task_id].processId = int(process_id)
    tasks[task_id].processName = processes[process_id].name

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

    thread_id = int(info[4])
    # For threads which aren't registered, they may have no name.
    if info[4] not in threads:
      threads[info[4]] = Thread(thread_id, '')
    tasks[task_id].threadId = thread_id
    tasks[task_id].threadName = threads[info[4]].name
  elif log_type == 2:
    tasks[task_id].end = timestamp
    tasks[task_id].executionTime = tasks[task_id].end - tasks[task_id].begin
  elif log_type == 3:
    tasks[task_id].add_label(timestamp, info[3])

def parse_log(log, process_id):
  """
    Parse log line by line and verify the parsing results based on the log type.
    Then, set up task information with the verified parsing results.
  """
  for line in log:
    info = None
    try:
      # Get log type
      [log_type, remain] = find_char_and_split(line.strip(), ' ', 1)

      # log_type:
      #   0 - DISPATCH. Ex. 0 taskId dispatch sourceEventId sourceEventType parentTaskId
      #   1 - BEGIN.    Ex. 1 taskId begin processId threadId
      #   2 - END.      Ex. 2 taskId end
      #   3 - LABEL.    Ex. 3 taskId timestamp "label"
      #   4 - VPTR.     Ex. 4 address
      if log_type == '3':
        [task_id, timestamp, remain] = find_char_and_split(remain, ' ', 2)
        info = [int(log_type), task_id, timestamp, remain.replace("\"", "")]
      else:
        tokens = find_char_and_split(remain)
        info = [int(log_type)] + tokens
    
      verify_info(info)
    except ParseError as error:
      error.log = line.strip()
      raise

    set_task_info(info, process_id)

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
  parser.add_argument('-l', '--input-log-folder', help='Input log folder', default='log')
  parser.add_argument('-m', '--input-mmap-folder', help='Input mmap folder', default='mmap')
  parser.add_argument('-sp', '--symbol-path', help='libxul.so path', required=True)
  parser.add_argument('-np', '--nm-path', help='nm path', required=True)
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
                 'taskId INT, threadId INT, begin INT, end INT, name TEXT, sourceEventId INT)'))

    # Insert information into table
    for task_id, task_obj in tasks.iteritems():
      insert_cmd = ('INSERT INTO Tasks VALUES({}, {}, {}, {}, \'{}\', {})'.format(task_id,
                    task_obj.threadId, task_obj.begin, task_obj.end,
                    task_obj.name, task_obj.sourceEventId))
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
           'threadId: {}, '.format(task_obj.threadId) +
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
        lo = mid
      elif (midval > x):
        hi = mid
      else:
        return address[mid][1]

def retrieve_task_name(nm_path, symbol_path):
  """Retrieve symbol from libxul.so"""
  p1 = subprocess.Popen([nm_path, '-a', symbol_path], stdout=subprocess.PIPE)
  p2 = subprocess.Popen(['grep', '_Z'], stdin=p1.stdout, stdout=subprocess.PIPE)
  p3 = subprocess.Popen(['c++filt'], stdin=p2.stdout, stdout=subprocess.PIPE)
  p4 = subprocess.Popen(['sort'], stdin=p3.stdout, stdout=subprocess.PIPE)
  p1.stdout.close()
  p2.stdout.close()
  p3.stdout.close()
  output = p4.communicate()[0]
  all_symbols = find_char_and_split(output, '\n')

  address = []
  for line in all_symbols:
    if len(line) == 0:
      continue

    try:
      tokens = find_char_and_split(line, ' ', 2)
    except ParseError:
      raise

    if len(tokens[0]) == 0:
      continue

    address.append((int(tokens[0], 16), tokens[2].strip()))

  # Get name for each task
  for task_id, task_obj in tasks.iteritems():
    if not (task_obj._vptr and task_obj.processId and
      processes[str(task_obj.processId)]._mem_offset):
      if show_warnings:
        print 'Skip task {} because of incomplete mem_offset.'.format(task_id)
      continue

    offset = task_obj._vptr - processes[str(task_obj.processId)]._mem_offset

    task_obj.name = binary_search(address, offset)

    if not task_obj.name:
      print task_obj._vptr, processes[str(task_obj.processId)]._mem_offset, offset

def read_log(input_folder):
  """
    Iterate through input folder and read all log files. Process information are
    set according to the filename. Task information and thread information are
    included in the json file.
  """
  for filename in listdir(input_folder):
    if not filename.startswith('profile') or not isfile(input_folder + '/' + filename):
      raise ReadError('Unrecognized log file: ' + filename)

    # Set up process info. Example filename: profile_3810_b2g.txt
    [name, ext] = find_char_and_split(filename, '.', 1)
    [prefix, process_id, process_name] = find_char_and_split(name, "_")
    processes[process_id] = Process(int(process_id), process_name)

    # Load json file and get tasktracer log.
    with open(input_folder + '/' + filename, 'r') as json_file:
      json_data = json.load(json_file)
      task_info = json_data["tasktracer"]["data"]
      thread_info = json_data["tasktracer"]["threads"]

    # Set up thread info.
    for t in thread_info:
      thread_id = t["tid"]
      thread_name = t["name"]
      # FIXME
      if thread_name == 'GeckoMain':
        thread_id = int(process_id)
      threads[thread_id] = Thread(thread_id, thread_name)

    parse_log(task_info, process_id)

def read_mmap(input_folder):
  for filename in listdir(input_folder):
    [prefix, process_id] = find_char_and_split(filename, '_')
    if str(process_id) not in processes:
      continue
  
    with open(input_folder + '/' + filename, 'r') as mmap_file:
      mmap_data = mmap_file.readlines()

    for line in mmap_data:
      if 'libxul.so' in line:
        [mem_offset, others] = find_char_and_split(line, '-', 1);
        processes[str(process_id)]._mem_offset = int(mem_offset, 16)
        break

def main(argv=sys.argv[:]):
  args = get_arguments(argv)

  print 'Input log folder:', args.input_log_folder
  print 'Input mmap folder:', args.input_mmap_folder
  print 'symbol path: ', args.symbol_path
  print 'nm path: ', args.nm_path
  print 'Output: task_tracer_data.json'

  global show_warnings
  show_warnings = args.show_warnings

  log = []
  try:
    read_log(args.input_log_folder)
    print len(tasks), 'tasks has been created successfully.'

    read_mmap(args.input_mmap_folder)
    retrieve_task_name(args.nm_path, args.symbol_path)
  except ParseError as error:
    print error.msg
    if error.log:
      print '@line: \'{}\''.format(error.log)
    sys.exit()
  except ReadError as error:
    print error.msg
    sys.exit()

  if args.check_parent_task_id:
    check_parent_task_id()

  if len(tasks) == 0:
    sys.exit()

  [begin_time, end_time] = retrieve_begin_end_time();

  output_json('task_tracer_data.json', begin_time, end_time)

  print len(tasks), 'tasks has been written to JSON output successfully.'
  if args.print_all_tasks:
    print_all_tasks()

if __name__ == '__main__':
  sys.exit(main())

