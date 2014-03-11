#!/bin/bash


LOGCAT_PID="./_logcat_pid"
DEFAULT_LOG_FILE="./task_tracer_data.logcat"

declare -a B2G_PIDS


###########################################################################
#
# Start logging TaskTracer with adb logcat, and generates the log file
#
HELP_start="Starts logging TaskTracer and writing logs to a file.
              -f [path/to/logfile], default to $DEFAULT_LOG_FILE if not specified."
cmd_start() {
  filename=${DEFAULT_LOG_FILE}
  
  while getopts ":f:" opt "$@";
  do
    case $opt in
      f)
        filename=$OPTARG
        ;;
    esac
  done  

  echo "Clear the logcat buffer, and start writing the TaskTracer log to:"
  echo "----> $filename"
  adb logcat -c
  nohup adb logcat -s TaskTracer > $filename 2>&1 &
  echo $! > ${LOGCAT_PID}
  echo "Started"
  
}

###########################################################################
#
HELP_stop="Stops writing logs to file."
cmd_stop() {
  kill -9 `cat ${LOGCAT_PID}`
  rm ${LOGCAT_PID} 

  echo "Stop writing log file."
}

###########################################################################
#
HELP_get_mmaps="Retrieve memory maps of b2g processes from device,
              and save those files to the current folder."
cmd_get_mmaps() {
  echo "Retrieving memory maps of b2g and its content processes from device..."
  unset B2G_PIDS
  
  B2G_PIDS=($(adb shell toolbox ps | while read line; do
    if [ "${line/*b2g*/b2g}" = "b2g" ]; then
      echo ${line} | (
        read user pid rest;
        echo -n "${pid} "
      )
    fi
  done))

  for pid in ${B2G_PIDS[*]}; do
    filename="./mmaps/mmaps_"${pid}
    adb shell cat /proc/${pid}/maps > $filename
    echo "Memory maps of "${pid}" saved to file: "$filename
  done
  
  echo "Done!"
}

###########################################################################
#
HELP_help="Shows these help messages"
cmd_help() {
  if [ "$1" == "" ]; then
    echo "Usage: ${SCRIPT_NAME} command [args]"
    echo "where command is one of:"
    for command in ${allowed_commands}; do
      desc=HELP_${command}
      printf "  %-11s %s\n" ${command} "${!desc}"
    done
  else
    command=$1
    if [ "${allowed_commands/*${command}*/${command}}" == "${command}" ]; then
      desc=HELP_${command}
      printf "%-11s %s\n" ${command} "${!desc}"
    else
      echo "Unrecognized command: '${command}'"
    fi  
  fi  
}

###########################################################################
#
# Determine if the first argument is a valid command and execute the
# corresponding function if it is.
#
allowed_commands=$(declare -F | sed -ne 's/declare -f cmd_\(.*\)/\1/p' | tr "\n" " ")
command=$1
if [ "${command}" == "" ]; then
  cmd_help
  exit 0
fi
if [ "${allowed_commands/*${command}*/${command}}" == "${command}" ]; then
  shift
  cmd_${command} "$@"
else
  echo "Unrecognized command: '${command}'"
fi

