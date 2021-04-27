
/*****hemorrhage 확인*******/

--intime, intime으로 부터 12시간 차이 계산해서 컬럼 추가
select *, DATEADD(hh,12,intime) as intime_12h into #bleeding_PRBC from PRBC_icu_join_RN

--starttime이 중환자실 입실 12시간 이후에 있는 경우
select * from #bleeding_PRBC
where intime_12h <= starttime 
order by subject_id, stay_id, starttime

--stay_id별 수혈 시작시간으로 정렬, 이전 수혈 시작 시간(lag)과 시간 차이 계산
select *, LAG(starttime, 1, NULL) over (partition by stay_id order by starttime) as pre_starttime into #lag_PRBC 
from (select * from #bleeding_PRBC
where intime_12h <= starttime) v
order by subject_id, stay_id, starttime

select *, DATEDIFF(hour, pre_starttime, starttime) as pre_diff into #lag_temp from #lag_PRBC
order by subject_id, stay_id, starttime

select * into #lag_temp_2 from #lag_temp
where pre_diff <= 24
or pre_diff is null
order by subject_id, stay_id, starttime

--그 중에서 수혈량이 PRBC 3unit(350*3) 이상인 경우만 stay_id 리스트업(2203)
select stay_id into #hemorrhage_id from #lag_temp_2
group by stay_id
having sum(amount)>=1050

select B.* into #hemo_temp from #hemorrhage_id A left join #lag_temp_2 B
on A.stay_id = B.stay_id

select * from #hemo_temp
order by subject_id, stay_id, starttime

--입실 12시간 이후에 첫 수혈이 시작된 경우(PRBC_RN=1) 있는 경우만 

--14980건
select *, row_number() over (partition by stay_id order by starttime) as hemo_RN into #hemo_temp_2 from #hemo_temp

--1188
select distinct stay_id from #hemo_temp_2
where PRBC_RN = hemo_RN

select * into #hemo_temp_3 from #hemo_temp_2
where PRBC_RN = hemo_RN
order by subject_id, stay_id, starttime

--그 중에서 수혈량이 PRBC 3unit(350*3) 이상인 경우만 stay_id 재 리스트업(2203)
select stay_id into #hemo_temp_id from #hemo_temp_3
group by stay_id
having sum(amount)>=1050

select B.* into #hemo_temp_4 from  #hemo_temp_id A left join #hemo_temp_3 B
on A.stay_id = B.stay_id


--771건 stay_id
select distinct stay_id from #hemo_temp_4

select subject_id, stay_id, intime, outtime, intime_12h, starttime, endtime, amount, pre_starttime, pre_diff, PRBC_RN, hemo_RN, los from #hemo_temp_4
order by subject_id, stay_id, starttime


--hemorrhage table로 
select * into hemorrhage from #hemo_temp_4
order by subject_id, stay_id, starttime
