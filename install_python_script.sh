#!/bin/bash

if [ $# -eq 0 -o $# -gt 2 ]; then
    echo 'ERROR: need 1 or 2 arguments !'
    echo "usage: $0 <script-path> [<conda-env>]"
    exit 1
fi

ROOT_DIR=$( dirname "$0" )
cd "${ROOT_DIR}"
ROOT_DIR=$( pwd )

script="$1"
script_path="${ROOT_DIR}/${script}"
script_name=$( basename "$1" | sed -e 's/[.]py$//' )

if [ $# -eq 2 ]; then
    env_name="$2"
else
    env_name="python3"
fi


echo "script name = ${script_name}"
echo "env name = ${env_name}"

if [ ! -f "${script_path}" ]; then
    echo "ERROR: Cannot find python script ${script_path}"
    exit 2
fi

ssh_key="${ROOT_DIR}/sshkeys/${script_name}.pub"
if [ -f "${ssh_key}" ]; then
    echo "ERROR: sshkey already exist, and command may already be installed (check authorized_keys2)"
    exit 3
fi

/bin/bash ${ROOT_DIR}/ssh_keygen.sh "${script_name}" >/dev/null 2>/dev/null

ssh_original_command='\"${SSH_ORIGINAL_COMMAND}\"'

# add empty line
echo '' >> authorized_keys2
echo -n 'restrict' >> authorized_keys2
echo -n ",command=\"/bin/bash /home/airflow/python_env.sh ${env_name} ${script_path} ${ssh_original_command}\" " >> authorized_keys2
cat "${ROOT_DIR}/sshkeys/${script_name}.pub" >> authorized_keys2

echo 'Private ssh key for this process :'
cat "${ROOT_DIR}/sshkeys/${script_name}"

