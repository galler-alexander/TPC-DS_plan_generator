#!/bin/bash

set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh

GEN_DATA_SCALE=$1
session_id=$2
E9=$3

if [[ "$GEN_DATA_SCALE" == "" || "$session_id" == "" || "$E9" == "" ]]; then
	echo "Error: you must provide the scale, the session id, and E9 true or false as parameters."
	echo "Example: ./rollout.sh 3000 2 false"
	echo "This will execute the TPC-DS queries for 3TB of data for session 2 that are dynamically."
	echo "created with dsqgen.  The E9 option will use the static queries and is only valid for 5 sessions."
	exit 1
fi

source_bashrc

step=testing_$session_id

init_log $step

if [ "$E9" == "true" ]; then
	sql_dir=$PWD/e9/$session_id
else
	sql_dir=$PWD/$session_id
	#going from 1 base to 0 base
	tpcds_id=$((session_id-1))
	tpcds_query_name="query_""$tpcds_id"".sql"
	query_id=1
	for p in $(seq 1 99); do
		q=$(printf %02d $query_id)
		template_filename=query$p.tpl
		start_position=""
		end_position=""
		for pos in $(grep -n $template_filename $sql_dir/$tpcds_query_name | awk -F ':' '{print $1}'); do
			if [ "$start_position" == "" ]; then
				start_position=$pos
			else
				end_position=$pos
			fi
		done

		#get the query number (the order of query execution) generated by dsqgen
		file_id=$(sed -n "$start_position","$start_position"p $sql_dir/$tpcds_query_name | awk -F ' ' '{print $4}')
		file_id=$(($file_id+100))
		filename=$file_id.query.$q.sql
		sed -n "$start_position","$end_position"p $sql_dir/$tpcds_query_name > $sql_dir/$filename
		query_id=$(($query_id + 1))
		echo "Completed: $sql_dir/$filename"
	done
	echo "rm -f $sql_dir/query_*.sql"
	rm -f $sql_dir/$tpcds_query_name
fi

tuples="0"
for i in $(ls $sql_dir/*.sql); do

	start_log
	id=$i
	schema_name=$session_id
	table_name=$(basename $i | awk -F '.' '{print $3}')

	echo "psql -A -q -t -P pager=off -v ON_ERROR_STOP=ON -f $i | wc -l"
	tuples=$(psql -A -q -t -P pager=off -v ON_ERROR_STOP=ON -f $i | wc -l; exit ${PIPESTATUS[0]})
	#remove the extra line that \timing adds
	tuples=$(($tuples-1))
	log $tuples
done

end_step $step
