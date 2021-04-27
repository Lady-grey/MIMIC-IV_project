
/*****hemorrhage Ȯ��*******/

--intime, intime���� ���� 12�ð� ���� ����ؼ� �÷� �߰�
select *, DATEADD(hh,12,intime) as intime_12h into #bleeding_PRBC from PRBC_icu_join_RN

--starttime�� ��ȯ�ڽ� �Խ� 12�ð� ���Ŀ� �ִ� ���
select * from #bleeding_PRBC
where intime_12h <= starttime 
order by subject_id, stay_id, starttime

--stay_id�� ���� ���۽ð����� ����, ���� ���� ���� �ð�(lag)�� �ð� ���� ���
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

--�� �߿��� �������� PRBC 3unit(350*3) �̻��� ��츸 stay_id ����Ʈ��(2203)
select stay_id into #hemorrhage_id from #lag_temp_2
group by stay_id
having sum(amount)>=1050

select B.* into #hemo_temp from #hemorrhage_id A left join #lag_temp_2 B
on A.stay_id = B.stay_id

select * from #hemo_temp
order by subject_id, stay_id, starttime

--�Խ� 12�ð� ���Ŀ� ù ������ ���۵� ���(PRBC_RN=1) �ִ� ��츸 

--14980��
select *, row_number() over (partition by stay_id order by starttime) as hemo_RN into #hemo_temp_2 from #hemo_temp

--1188
select distinct stay_id from #hemo_temp_2
where PRBC_RN = hemo_RN

select * into #hemo_temp_3 from #hemo_temp_2
where PRBC_RN = hemo_RN
order by subject_id, stay_id, starttime

--�� �߿��� �������� PRBC 3unit(350*3) �̻��� ��츸 stay_id �� ����Ʈ��(2203)
select stay_id into #hemo_temp_id from #hemo_temp_3
group by stay_id
having sum(amount)>=1050

select B.* into #hemo_temp_4 from  #hemo_temp_id A left join #hemo_temp_3 B
on A.stay_id = B.stay_id


--771�� stay_id
select distinct stay_id from #hemo_temp_4

select subject_id, stay_id, intime, outtime, intime_12h, starttime, endtime, amount, pre_starttime, pre_diff, PRBC_RN, hemo_RN, los from #hemo_temp_4
order by subject_id, stay_id, starttime


--hemorrhage table�� 
select * into hemorrhage from #hemo_temp_4
order by subject_id, stay_id, starttime
