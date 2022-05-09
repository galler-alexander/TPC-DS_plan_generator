#!/bin/bash
set -e

PWD=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
source $PWD/../functions.sh
source_bashrc

GEN_DATA_SCALE=$1
EXPLAIN_ANALYZE=$2
RANDOM_DISTRIBUTION=$3
MULTI_USER_COUNT=$4
SINGLE_USER_ITERATIONS=$5

if [[ "$GEN_DATA_SCALE" == "" || "$EXPLAIN_ANALYZE" == "" || "$RANDOM_DISTRIBUTION" == "" || "$MULTI_USER_COUNT" == "" || "$SINGLE_USER_ITERATIONS" == "" ]]; then
	echo "You must provide the scale as a parameter in terms of Gigabytes, true/false to run queries with EXPLAIN ANALYZE option, true/false to use random distrbution, multi-user count, and the number of sql iterations."
	echo "Example: ./rollout.sh 100 false false 5 1"
	exit 1
fi

step=sql
init_log $step

rm -f $PWD/../log/*single.explain.*.log
rm -f $PWD/../log/*single.explain_analyzei.*.log
for i in $(ls $PWD/*.tpcds.*.sql); do
	for x in $(seq 1 $SINGLE_USER_ITERATIONS); do
		id=`echo $i | awk -F '.' '{print $1}'`
		schema_name=`echo $i | awk -F '.' '{print $2}'`
		table_name=`echo $i | awk -F '.' '{print $3}'`
		start_log
		myfilename=$(basename $i)
		if [ "$EXPLAIN_ANALYZE" == "false" ]; then
			mylogfile=$PWD/../log/$myfilename.single.explain.text.log
			echo "psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE=\"EXPLAIN (VERBOSE, COSTS, FORMAT TEXT)\" -f $i > $mylogfile"
			psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE="EXPLAIN (VERBOSE, COSTS, FORMAT TEXT)" -f $i > $mylogfile
			tuples="0"
		fi
		mylogfile=$PWD/../log/$myfilename.single.explain_explain.text.log
		echo "psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE=\"EXPLAIN (ANALYZE, VERBOSE, COSTS, BUFFERS, TIMING, FORMAT TEXT) \" -f $i > $mylogfile"
		psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE="EXPLAIN (ANALYZE, VERBOSE, COSTS, BUFFERS, TIMING, FORMAT TEXT)" -f $i > $mylogfile
		# TODO
		#tuples=$(psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE="" -f $i | wc -l; exit ${PIPESTATUS[0]})
		tuples="0"

		if [ "$EXPLAIN_ANALYZE" == "false" ]; then
			mylogfile=$PWD/../log/$myfilename.single.explain.json.log
			echo "psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE=\"EXPLAIN (VERBOSE, COSTS, FORMAT JSON)\" -f $i > $mylogfile"
			psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE="EXPLAIN (VERBOSE, COSTS, FORMAT JSON)" -f $i > $mylogfile
			tuples="0"
		fi
		mylogfile=$PWD/../log/$myfilename.single.explain_explain.json.log
		echo "psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE=\"EXPLAIN (ANALYZE, VERBOSE, COSTS, BUFFERS, TIMING, FORMAT JSON) \" -f $i > $mylogfile"
		psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE="EXPLAIN (ANALYZE, VERBOSE, COSTS, BUFFERS, TIMING, FORMAT JSON)" -f $i > $mylogfile
		# TODO
		#tuples=$(psql -v ON_ERROR_STOP=1 -A -q -t -P pager=off -v EXPLAIN_ANALYZE="" -f $i | wc -l; exit ${PIPESTATUS[0]})
		tuples="0"

		log $tuples
	done
done

end_step $step
