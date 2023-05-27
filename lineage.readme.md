# Flink SQL Lineage
##todo
###Event Time Temporal Join
###Processing Time Temporal Join


##test pass
### cep sql
insert into ekyc_dashboard_agent_connect_result (agent_id,room_id,application_id,type,begin_time,end_time) select agent_id,room_id,application_id,type,begin_time,end_time from ekyc_dashboard_agent_connect match_recognize (partition by agent_id,room_id,application_id order by row_time measures AF.type as type,last(BF.create_time) as begin_time,last(AF.create_time) as end_time one row per match after match SKIP PAST LAST ROW pattern (BF+ AF) WITHIN INTERVAL '1' HOUR define BF as BF.type = 'assign',AF as AF.type = 'pick_up' ) as T

### lookup join
insert into dm_keystat_payment(type, txn, createTimestamp) select cast('txn_out_ewallets_excl_spp' as string) as type,tran_amt as txn, a.createTimestamp from rt_pmt_core_t_tran_txn_msg as a join ods.pts_pmt_core_t_bank_info_ss_d  for system_time as of a.proctime as b on a.payee_bank_code=b.bank_id where b.bank_type = 2 and ((a.direction = 'O' and a.booking_type='C') or (a.direction = 'I' and a.booking_type='D')) and a.biz_status = 'S' and a.passage_id = 'ALTOPAY'

### wtf
insert into user_ccu(batch_time, os, batch_value)  select cast(window_end as string), os, cast(count(1) as string) from table(tumble(table user_center_ccu, DESCRIPTOR(createTimestamp),INTERVAL '5' MINUTE)) as a where CHARACTER_LENGTH(a.uid) > 0  group by window_start,window_end,os

### top N
insert into user_ccu(batch_time, os, batch_value) select t,os,num from (select cast(window_end as string) as t, os, cast(count(1) as string) as num, row_number() over (partition by window_end order by createTimestamp) as rownum from table(tumble(table user_center_ccu, DESCRIPTOR(createTimestamp),INTERVAL '5' MINUTE)) group by window_start,window_end,os,createTimestamp) where rownum=1

### top N + wtf
insert into user_ccu(batch_time, os, batch_value) select t,os,num from (select cast(window_end as string) as t, os, cast(count(1) as string) as num, row_number() over (partition by window_end order by createTimestamp) as rownum from table(tumble(table user_center_ccu, DESCRIPTOR(createTimestamp),INTERVAL '5' MINUTE)) group by window_start,window_end,os,createTimestamp) where rownum=1