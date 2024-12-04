#!/usr/bin/env python3

print("# groupby-datafusion.py", flush=True)

import os
import gc
import timeit
import datafusion as df
from datafusion import functions as f
from datafusion import col
from pyarrow import csv as pacsv

exec(open("./_helpers/helpers.py").read())

def ans_shape(batches):
    rows, cols = 0, 0
    for batch in batches:
        rows += batch.num_rows
        if cols == 0:
            cols = batch.num_columns
        else:
            assert(cols == batch.num_columns)
    
    return rows, cols

ver = df.__version__
git = ""
task = "groupby"
solution = "datafusion"
fun = ".groupby"
cache = "TRUE"
on_disk = "FALSE"

data_name = os.environ["SRC_DATANAME"]
src_grp = os.path.join("data", data_name + ".csv")
print("loading dataset %s" % data_name, flush=True)

data = pacsv.read_csv(src_grp, convert_options=pacsv.ConvertOptions(auto_dict_encode=True))

ctx = df.SessionContext()
ctx.register_record_batches("x", [data.to_batches()])

in_rows = data.num_rows
print(in_rows, flush=True)

task_init = timeit.default_timer()

question = "sum v1 by id1" # q1
gc.collect()

t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, SUM(v1) AS v1 FROM x GROUP BY id1").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()


question = "sum v1 by id1:id2" # q2
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, SUM(v1) AS v1 FROM x GROUP BY id1, id2").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()


question = "sum v1 mean v3 by id3" # q3
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, SUM(v1) AS v1, AVG(v3) AS v3 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "mean v1:v3 by id4" # q4
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id4, AVG(v1) AS v1, AVG(v2) AS v2, AVG(v3) AS v3 FROM x GROUP BY id4").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()


question = "sum v1:v3 by id6" # q5
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, SUM(v1) AS v1, SUM(v2) AS v2, SUM(v3) AS v3 FROM x GROUP BY id6").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v1")), f.sum(col("v2")), f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "median v3 sd v3 by id4 id5" # q6
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id4, id5, MEDIAN(v3) AS median_v3, STDDEV(v3) AS sd_v3 FROM x GROUP BY id4, id5").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("median_v3")),f.sum(col("sd_v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id4, id5, MEDIAN(v3) AS median_v3, STDDEV(v3) AS sd_v3 FROM x GROUP BY id4, id5").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("median_v3")),f.sum(col("sd_v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "max v1 - min v2 by id3" # q7
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("range_v1_v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id3, MAX(v1) - MIN(v2) AS range_v1_v2 FROM x GROUP BY id3").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("range_v1_v2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()


question = "largest two v3 by id6" # q8
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, v3 from (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS row FROM x) t WHERE row <= 2").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id6, v3 from (SELECT id6, v3, row_number() OVER (PARTITION BY id6 ORDER BY v3 DESC) AS row FROM x) t WHERE row <= 2").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "regression v1 v2 by id2 id4" # q9
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id2, id4, POW(CORR(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("r2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id2, id4, POW(CORR(v1, v2), 2) AS r2 FROM x GROUP BY id2, id4").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("r2"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()

question = "sum v3 count by id1:id6" # q10
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, id3, id4, id5, id6, SUM(v3) as v3, COUNT(*) AS cnt FROM x GROUP BY id1, id2, id3, id4, id5, id6").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3")), f.sum(col("cnt"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=1, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()
t_start = timeit.default_timer()
ans = ctx.sql("SELECT id1, id2, id3, id4, id5, id6, SUM(v3) as v3, COUNT(*) AS cnt FROM x GROUP BY id1, id2, id3, id4, id5, id6").collect()
shape = ans_shape(ans)
print(shape, flush=True)
t = timeit.default_timer() - t_start
m = memory_usage()
t_start = timeit.default_timer()
df = ctx.create_dataframe([ans])
chk = df.aggregate([], [f.sum(col("v3")), f.sum(col("cnt"))]).collect()[0].column(0)[0]
chkt = timeit.default_timer() - t_start
write_log(task=task, data=data_name, in_rows=in_rows, question=question, out_rows=shape[0], out_cols=shape[1], solution=solution, version=ver, git=git, fun=fun, run=2, time_sec=t, mem_gb=m, cache=cache, chk=make_chk([chk]), chk_time_sec=chkt, on_disk=on_disk)
del ans
gc.collect()


print("grouping finished, took %0.fs" % (timeit.default_timer() - task_init), flush=True)

exit(0)
