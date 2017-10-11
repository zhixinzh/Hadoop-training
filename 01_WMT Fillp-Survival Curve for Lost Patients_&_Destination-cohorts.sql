/**********************************************************************************************************************/ 
--PROGRAM			: WMT Fillp-WMT Fillp-Survival Curve for Lost Patients_&_Destination-All cohorts.sql
--PURPOSE			: To build Survival Curves for All patinets using the new 6 months gap method for Top 10 
--						USC3 market and specifying maintenance drugs which are also not in Specialty market and 
--						then also find these lost patients' destination (comp/mail/stopped treatment): 
--						Supercenter VS NHM for 2015 cohorts
--CLIENT			: WMT
--PROJECT			: Research-WMT Patient Fill Pattern
--PROGRAMMER		: Zhang, Zhixin; Tu,Xiaoran
--METHODOLOGIST		: Chen, Joyce
--CREATED			: 2017-09-27
--COMMENTS			: This project is developed under Hadoop Impala table PROD_DF2_SR3_dpt
/**********************************************************************************************************************/


/*****************************************/ 
/*****************************************/ 
--Change on 2017/09/18 
--1.Add one criterion for cohort selection: 
--   patients must also have at least two scripts in 18 months(cohort month+17 months looking forward)
--2.Refresh Diabetes data using the IMS standard market definition instead of USC3 39200

--Change on 2017/09/27
--Delete pat_value_ind and psycho_vars


/*****************************************/ 
/*****************************************/ 
--Create market definition file for top10 USC class;
--Note: For Diabetes market, use the Jan's market definition for Jul17, link with the NDC-level table and CMF10-level table to get the convert the market definition to cmf10 level;
drop table if exists prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf purge;
create table prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf stored as parquet 
as 
with t1 as (
			select distinct
				 usc_3_cd
				,usc_3_desc
				,prod_desc
				,cast(concat(LPAD(cast(prod_id as string),7,'0'),lpad(cast(PACK_CD as string),3,'0')) as bigint) as cmf10	
			from 
				prod_df2_sr3.SR3_SMD_RSLVD_PPK
			where (spcl_prod_ind <>'9' or spcl_prod_ind is null)
				and usc_3_cd in ('31100','32100','31400','64300','41100','23400','31300','20200','72100') 
			),
			--Temp table: including other 9 USC3 market
	 t2 as (
			select 
				distinct 
				'39200' as usc_3_cd
				,'DIABETES'
				,prod_desc
				,cast(concat(LPAD(cast(ppk.prod_id as string),7,'0'),lpad(cast(ppk.PACK_CD as string),3,'0'))as bigint) as cmf10
			from 
				(select distinct ndc from prod_df2_sr3_dpt.jbk_diabetes_mkt_jul17) mdf, 
				prod_df2_sr3.sr3_smd_ndc_detl prod, prod_df2_sr3.sr3_smd_rslvd_ppk ppk
			where lpad(mdf.ndc,12,'0') = prod.ndc_cd 
				and prod.prod_id = ppk.prod_id
				and prod.pack_cd = ppk.pack_cd
				and (ppk.spcl_prod_ind <>'9' or ppk.spcl_prod_ind is null)
			)	
			--Temp table: including Diabetes market using PQA mdf
select * from t1
union all
select * from t2;
compute stats prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf;

--QC
select usc_3_cd, usc_3_desc,count(*) as cmf10_cnt 
from prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf
group by usc_3_cd,usc_3_desc
order by usc_3_cd,usc_3_desc
;
select * from prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf
;


/*****************************************/ 
/*****************************************/ 
--Extract TOP10 USC3 Cohort patients between 201501 to 201512
--1. maint_non_spcl for Supercenter: cohort is defined as any patient had at least one maint_non_spcl Rx at Supercenter on Jan 2015 to Dec 2015 (no therapy specified)
--2. maint_non_spcl for Neighborhood Market: cohort is defined as any patient had at least one maint_non_spcl Rx at NBH on Jan 2015 to Dec 2015 (no therapy specified)

--COHORT that finally decided to use: Join prod_df2_sr3.SR3_SMD_RSLVD_PPK non '9' products (0 or null) 
--on lrx.cmf10=cast(concat(LPAD(cast(prod.prod_id as string),7,'0'),lpad(cast(prod.PACK_CD as string),3,'0'))as bigint)


/*****************************************/ 
--Refresh Cohort: Since Diabetes mdf has been updated;
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp stored as parquet as 
select 
	  distinct 
	  prod.usc_3_cd 
	, prod.usc_3_desc
	, lrx.xref_ims_pat_nbr
	, lrx.org_cd
	, lrx.ims_yyyymm_dspnsd_dt as cohort_ims_yyyymm_dspnsd_dt
	, CAST(concat(substr(cast(lrx.ims_yyyymm_dspnsd_dt as string),1,4) ,'-',lpad(substr(cast(lrx.ims_yyyymm_dspnsd_dt as string),5,2),2,'0') ,'-01') AS timestamp) as cohort_month
from  
	prod_df2_cls.CLS_CUSTM_EXTRACT_TRANS lrx,
	prod_df2_sr3.xr_wmt_lpp_top10_usc_mdf prod
where lrx.cmf10=prod.cmf10
	and lrx.ims_yyyymm_dspnsd_dt between 201501 and 201512
	and lrx.otlt_chnl_ind in ('RETAIL')          
	and lrx.misbridge_pat_flag <> 1
	and lrx.ims_supplier_id <> 725 
	and lrx.ST_CD not in ('VI','PR')
	and lrx.org_cd in ('032','955')
	and lrx.maint_cmf10=1
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp; --159907396

--QC
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,org_cd, count(1) as rx_cnt, count(distinct xref_ims_pat_nbr) as pat_cnt
from prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp group by 1,2,3,4 order by 1,2,3,4
;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp limit 30
;


/*****************************************/ 
--Extract all Rxs for cohort patients (2015 top 10 USC3 maint_non_spcl);
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_Allpat_rxs_T10USC3_cohorts_2015_maint_non_spcl purge;
create table prod_df2_sr3_dpt.zz_wmt_fillp_Allpat_rxs_T10USC3_cohorts_2015_maint_non_spcl stored as parquet 
as 
with raw as (
				select 
					 lrx.supplier_par_id
					,lrx.xref_ims_pat_nbr
					,lrx.otlt_ims_id
					,lrx.ims_dspnsd_dt
					,CAST(concat(CAST(extract(year from lrx.ims_dspnsd_dt) AS string),'-', lpad(CAST(extract(month from lrx.ims_dspnsd_dt) AS string),2,'0') ,'-01') AS timestamp) as Fistof_month
					,lrx.usc_cd
					,lrx.rx_qty
					,lrx.pay_typ_cd
					,lrx.pat_gender_cd
					,lrx.pat_age_nbr
					,lrx.ims_supplier_id
					,lrx.ims_yyyymm_dspnsd_dt
					,lrx.org_cd
					,lrx.ddsupp_cnt
					,lrx.cmf10
					,lrx.maint_cmf10
					,lrx.dspnsd_ndc
					,lrx.otlt_chnl_ind
					,lrx.mp_surro_id
					,lrx.mp_ims_id
					,lrx.mp_seq_ims_id
					,lrx.payer_pln_surro_id
					,lrx.payer_pln_curr_xref
					,lrx.mdel_typ_curr
					,lrx.st_cd
					,lrx.otlt_cot_ind
					,lrx.claim_id
					,case when LRX.ddsupp_cnt < 84 then 1   /** adjust 90 day RX ******/
					else 3 end  as Rxs_90dyAdj
					,prod.usc_3_cd
					,prod.usc_3_desc
					,prod.prod_desc
				from 
					prod_df2_cls.CLS_CUSTM_EXTRACT_TRANS lrx, 
					prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf prod
				WHERE lrx.cmf10=prod.cmf10
					and lrx.ims_yyyymm_dspnsd_dt between 201501 and 201705
					and lrx.otlt_chnl_ind in ('RETAIL')          
					and lrx.misbridge_pat_flag <> 1
					and lrx.ims_supplier_id <> 725 
					and lrx.ST_CD not in ('VI','PR')
					and lrx.org_cd in ('032','955')
					and lrx.maint_cmf10=1
			)
			--Raw rxs for cohort patients
select 
		raw.*,
		cht.org_cd as cohort_type,
		cht.cohort_ims_yyyymm_dspnsd_dt,
		cht.cohort_month
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp cht 
	inner join 
	raw 
where raw.xref_ims_pat_nbr=cht.xref_ims_pat_nbr
	and raw.org_cd=cht.org_cd
	and raw.usc_3_cd=cht.usc_3_cd
	and MONTHS_BETWEEN(raw.Fistof_month,cht.cohort_month)>=0
	and MONTHS_BETWEEN(raw.Fistof_month,cht.cohort_month)<=17
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_Allpat_rxs_T10USC3_cohorts_2015_maint_non_spcl;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_Allpat_rxs_T10USC3_cohorts_2015_maint_non_spcl; --1644466602


/*****************************************/ 
--Add new criterion on 2017/09/18; Update cohort

--Here we used a Dimention table which include cohort_month, lb_month and lf_month for each cohort 
drop table if exists prod_df2_sr3_dpt.xr_wmt_lpp_dim_period purge;
create table prod_df2_sr3_dpt.xr_wmt_lpp_dim_period stored as parquet 
as 
with t1 as (
            select ims_yyyymm_dspnsd_dt, min(ims_dspnsd_dt) as min_dt, max(ims_dspnsd_dt) as max_dt 
            from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp
            where ims_yyyymm_dspnsd_dt between 201501 and 201512 
            group by ims_yyyymm_dspnsd_dt
			)
			--Temp table: find cohort month info
select 
	ims_yyyymm_dspnsd_dt as cohort_month, 
	min_dt as cht_mth_beg,
	max_dt as cht_mth_end,
	add_months(min_dt,-12) as lb_beg, 
	date_sub(min_dt, 1) as lb_end,
	add_months(max_dt,17) as cht_lf_end
from 
	t1;
compute stats prod_df2_sr3_dpt.xr_wmt_lpp_dim_period;

--QC
select * from prod_df2_sr3_dpt.xr_wmt_lpp_dim_period order by  cohort_month;


----Patient must have at least two Rxs in 18 months(cohort + 17 months looking forward);
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3 purge;
create table prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3 stored as parquet 
as 
with t1 as (
			select 
				usc_3_cd, usc_3_desc, cohort_type, cohort_ims_yyyymm_dspnsd_dt, rx.cohort_month, xref_ims_pat_nbr, count(*) as rx_count 
			from 
				prod_df2_sr3_dpt.zz_wmt_fillp_Allpat_rxs_T10USC3_cohorts_2015_maint_non_spcl rx, 
				prod_df2_sr3_dpt.xr_wmt_lpp_dim_period dim_pd
			where rx.cohort_month = dim_pd.cht_mth_beg 
				and rx.ims_dspnsd_dt between dim_pd.cht_mth_beg and dim_pd.cht_lf_end 
			group by usc_3_cd, usc_3_desc, cohort_type, cohort_ims_yyyymm_dspnsd_dt, rx.cohort_month, xref_ims_pat_nbr
				having count(*) > 1
			) 
			--Patients who have at least two rxs in 18 months
select 
	cht.*
from 
		prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3_tmp cht 
	inner join 
		t1 
	on cht.usc_3_cd = t1.usc_3_cd
		and cht.org_cd = t1.cohort_type
		and cht.cohort_month = t1.cohort_month 
		and cht.xref_ims_pat_nbr = t1.xref_ims_pat_nbr
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3; --147823528


/*****************************************/ 
--Define pats type in the 2015 Top 10 USC3 maint_non_spcl cohorts by looking at 12 lookback periods
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_pat_status_T10USC3_cohorts_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_pat_status_T10USC3_cohorts_2015_maint_non_spcl_v3 stored as parquet as
select 
	cht2.*,
	case when ctn.xref_ims_pat_nbr is null then 'NEW'
	     else 'CONTINUING' 
		 end as pat_status
from 
		prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3 cht2
	left join 
		(
			select 
				distinct 
				cht.usc_3_cd 
				, cht.usc_3_desc
				, cht.org_cd as cohort_type
				, cht.cohort_ims_yyyymm_dspnsd_dt
				, cht.cohort_month
				, cht.xref_ims_pat_nbr
			from 
				(
					select 
						distinct
						lrx.xref_ims_pat_nbr
						--,lrx.ims_dspnsd_dt
						,CAST(concat(CAST(extract(year from lrx.ims_dspnsd_dt) AS string),'-', lpad(CAST(extract(month from lrx.ims_dspnsd_dt) AS string),2,'0') ,'-01') AS timestamp) as Fistof_lb_month
						,lrx.ims_yyyymm_dspnsd_dt
						,lrx.org_cd
						--,lrx.cmf10
						,lrx.maint_cmf10
						,lrx.otlt_chnl_ind
						--,lrx.st_cd
						,prod.usc_3_cd
						,prod.usc_3_desc
					from 
						prod_df2_cls.CLS_CUSTM_EXTRACT_TRANS lrx, 
						prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf prod
					WHERE lrx.cmf10=prod.cmf10
						and lrx.ims_yyyymm_dspnsd_dt between 201401 and 201511
						and lrx.otlt_chnl_ind in ('RETAIL')          
						and lrx.misbridge_pat_flag <> 1
						and lrx.ims_supplier_id <> 725 
						and lrx.ST_CD not in ('VI','PR')
						and lrx.org_cd in ('032','955')
						and lrx.maint_cmf10=1
				) lb
			inner join 
				prod_df2_sr3_dpt.zz_wmt_fillp_pat_T10USC3_cohorts_2015_maint_non_spcl_v3 cht
			on lb.xref_ims_pat_nbr=cht.xref_ims_pat_nbr
				and lb.org_cd=cht.org_cd
				and lb.usc_3_cd=cht.usc_3_cd
				and MONTHS_BETWEEN(cht.cohort_month,lb.Fistof_lb_month)>=1
				and MONTHS_BETWEEN(cht.cohort_month,lb.Fistof_lb_month)<=12
		) ctn
		--Patients who has rxs in their 12 months look back period
	on ctn.xref_ims_pat_nbr=cht2.xref_ims_pat_nbr
		and ctn.cohort_type=cht2.org_cd
		and ctn.usc_3_cd=cht2.usc_3_cd
		and ctn.cohort_ims_yyyymm_dspnsd_dt=cht2.cohort_ims_yyyymm_dspnsd_dt
		--where ctn.xref_ims_pat_nbr is null
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_status_T10USC3_cohorts_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_status_T10USC3_cohorts_2015_maint_non_spcl_v3;  --147823528 


/*****************************************/
/*****************************************/
--We will build the prepare Survival Curve separately for NEW and CONTINUING patients


/*********************************************************************************************************************************/
-- The following part is for NEW Patients
/*********************************************************************************************************************************/


/*****************************************/ 
--Find NEW pats in the 2015 Top 10 USC3 maint_non_spcl cohorts by looking at 12 lookback periods
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v3 stored as parquet as
select 
	cht.*
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_pat_status_T10USC3_cohorts_2015_maint_non_spcl_v3 cht
where pat_status = 'NEW'
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v3; --10105817

--QC
select usc_3_cd,usc_3_desc,cohort_month,org_cd, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v2 
group by 1,2,3,4 
order by 1,2,3,4; 

select usc_3_cd,usc_3_desc,cohort_month,org_cd, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v3 
group by 1,2,3,4 
order by 1,2,3,4; 


/*****************************************/ 
--Extract RXS for cohort NEW pats (new 2015 top 10 USC3 maint_non_spcl);
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3 stored as parquet as
select 
	rx.*
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_Allpat_rxs_T10USC3_cohorts_2015_maint_non_spcl rx, 
    prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v3 cht 
where rx.xref_ims_pat_nbr = cht.xref_ims_pat_nbr 
	and rx.org_cd = cht.org_cd 
	and rx.usc_3_cd = cht.usc_3_cd 
	and rx.cohort_month = cht.cohort_month
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3; --73548414

--QC on obs rxs
select usc_3_cd,usc_3_desc,cohort_month,cohort_type, 
       count(distinct xref_ims_pat_nbr) as pat_cnt, 
	  count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3 
group by 1,2,3,4 
order by 1,2,3,4;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3 limit 10
;


/*****************************************/ 
--Define 6 months gap for two cohort Patients
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_new_pat_gaps_T10USC3_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_new_pat_gaps_T10USC3_2015_maint_non_spcl_v3 stored as parquet as 
select *
, CAST(concat(CAST(extract(year from next_Fistof_month_2) AS string),
     lpad(CAST(extract(month from next_Fistof_month_2) AS string),2,'0')) AS int) as gap_end
, months_between(next_Fistof_month_2,Fistof_month) as month_gap
from 
	(
	select 
		chtrx.*
		, lag(chtrx.Fistof_month) over (partition by chtrx.usc_3_cd, chtrx.usc_3_desc ,chtrx.cohort_month, chtrx.cohort_ims_yyyymm_dspnsd_dt, chtrx.cohort_type, chtrx.xref_ims_pat_nbr 
								order by chtrx.gap_start desc) as next_Fistof_month
		, case when lag(chtrx.Fistof_month) over (partition by chtrx.usc_3_cd, chtrx.usc_3_desc ,chtrx.cohort_month, chtrx.cohort_ims_yyyymm_dspnsd_dt, chtrx.cohort_type, chtrx.xref_ims_pat_nbr 
								order by chtrx.gap_start desc) is null then add_months(cohort_month,18) 
				else lag(chtrx.Fistof_month) over (partition by chtrx.usc_3_cd, chtrx.usc_3_desc ,chtrx.cohort_month, chtrx.cohort_ims_yyyymm_dspnsd_dt, chtrx.cohort_type, chtrx.xref_ims_pat_nbr 
								order by chtrx.gap_start desc)
				end as next_Fistof_month_2
	from
		(
		select 
			usc_3_cd
			,usc_3_desc 
			,cohort_month
			,cohort_ims_yyyymm_dspnsd_dt
			,cohort_type
			,xref_ims_pat_nbr
			,Fistof_month
			,ims_yyyymm_dspnsd_dt as gap_start
			,count(1) as rx_cnt
			,sum(Rxs_90dyAdj) as adj_rx_cnt
		from 
			prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3
		group by 1,2,3,4,5,6,7,8
		) chtrx
	) lag
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pat_gaps_T10USC3_2015_maint_non_spcl_v3 ; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pat_gaps_T10USC3_2015_maint_non_spcl_v3 ; --65160231

--QC
select * from prod_df2_sr3_dpt.zz_wmt_fillp_new_pat_gaps_T10USC3_2015_maint_non_spcl_v3 order by 1,2,3,4 limit 30
;
select usc_3_cd,usc_3_desc, cohort_ims_yyyymm_dspnsd_dt, cohort_type, 
	  count(distinct xref_ims_pat_nbr) as pat_cnt, 
	  sum(rx_cnt) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_new_pat_gaps_T10USC3_2015_maint_non_spcl_v3 
group by 1,2,3,4 
order by 1,2,3,4
limit 250; 


/*****************************************/ 
--select new patients nearest 6 month gap
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 stored as parquet as
select 
	  cht.usc_3_cd
	, cht.usc_3_desc 
	, cht.cohort_month
	, cht.cohort_ims_yyyymm_dspnsd_dt
	, cht.org_cd as cohort_type 
	, cht.xref_ims_pat_nbr
	, gap.lastrx_before_lost
	, gap.before_gap_start
	, gap.after_6mth_gap_end
	, case when gap.lastrx_before_lost is null then 13
		else months_between(gap.lastrx_before_lost,cht.cohort_month)+1 
		end as lost_month
from 
	(
	select 
		  usc_3_cd
		, usc_3_desc
		, cohort_month
		, cohort_ims_yyyymm_dspnsd_dt
		, cohort_type
		, xref_ims_pat_nbr
		, min(Fistof_month) as lastrx_before_lost
		, min(gap_start) as before_gap_start
		,CAST(concat(CAST(extract(year from add_months(min(Fistof_month),7)) AS string),
			lpad(CAST(extract(month from add_months(min(Fistof_month),7)) AS string),2,'0')) AS int) as after_6mth_gap_end
	from 
		prod_df2_sr3_dpt.zz_wmt_fillp_new_pat_gaps_T10USC3_2015_maint_non_spcl_v3 
	where month_gap>=7 
	group by 1,2,3,4,5,6
	) gap 
right join 
	prod_df2_sr3_dpt.zz_wmt_fillp_pat_new_T10USC3_cohorts_2015_maint_non_spcl_v3 cht
on gap.usc_3_cd=cht.usc_3_cd
	and gap.xref_ims_pat_nbr=cht.xref_ims_pat_nbr
	and gap.cohort_type=cht.org_cd
	and gap.cohort_month=cht.cohort_month
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3; --10105817

--QC
select * from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 limit 10
;
select usc_3_cd, usc_3_desc,cohort_ims_yyyymm_dspnsd_dt, cohort_type, lost_month, count(1) as pat_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3
group by 1,2,3,4,5 
order by 1,2,3,4,5
limit 100;


/*****************************************/ 
--We will find lost patients' lost DESTINATION directly in this program
/*****************************************/ 
--Extract Lost patients' rxs in 6 months LF period
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 stored as parquet as 
select 
	pats.usc_3_cd
	,pats.usc_3_desc
	,pats.cohort_month
	,pats.cohort_ims_yyyymm_dspnsd_dt
	,pats.cohort_type
	,pats.xref_ims_pat_nbr
	,pats.lastrx_before_lost
	,pats.before_gap_start
	,pats.after_6mth_gap_end
	,pats.lost_month
	,raw.usc_cd
	,raw.ims_yyyymm_dspnsd_dt
	,raw.org_cd
	,raw.otlt_chnl_ind
	,case when raw.otlt_chnl_ind='MAIL' then 'Mail'
		when pats.cohort_type!=raw.org_cd and raw.otlt_chnl_ind='RETAIL' then 'Competitor'
		when pats.cohort_type=raw.org_cd then 'ATTENTION'
		when raw.org_cd is null then 'Stopped Treatment'
		end as lost_dest
	,case when raw.ddsupp_cnt < 84 then 1   /** adjust 90 day RX ******/
		else 3 end  as Rxs_90dyAdj
	,raw.ims_dspnsd_dt
	,raw.maint_cmf10
	,raw.st_cd
	,raw.otlt_cot_ind
	,raw.supplier_par_id
	,raw.otlt_ims_id
	,raw.rx_qty
	,raw.pay_typ_cd
	,raw.pat_gender_cd
	,raw.pat_age_nbr
	,raw.ims_supplier_id
	,raw.ddsupp_cnt
	,raw.dspnsd_ndc
	,raw.cmf10
	,raw.prod_desc
	,raw.mp_surro_id
	,raw.mp_ims_id
	,raw.mp_seq_ims_id
	,raw.payer_pln_surro_id
	,raw.payer_pln_curr_xref
	,raw.mdel_typ_curr
	,raw.claim_id
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 pats
left join 
	( 
	select 
		lrx.*,prod.usc_3_cd, 
		prod.prod_desc
	from 
		prod_df2_cls.CLS_CUSTM_EXTRACT_TRANS lrx,
		prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf prod
	WHERE lrx.cmf10=prod.cmf10
		and lrx.ims_yyyymm_dspnsd_dt between 201501 and 201705
		and lrx.otlt_chnl_ind in ('RETAIL','MAIL')          
		and lrx.misbridge_pat_flag <> 1
		and lrx.ims_supplier_id <> 725 
		and lrx.ST_CD not in ('VI','PR')
		and lrx.maint_cmf10=1
	) raw
on raw.usc_3_cd=pats.usc_3_cd
	and raw.xref_ims_pat_nbr=pats.xref_ims_pat_nbr
	and pats.before_gap_start<raw.ims_yyyymm_dspnsd_dt
	and pats.after_6mth_gap_end>raw.ims_yyyymm_dspnsd_dt
WHERE pats.lost_month<13
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3; --9228546

--QC
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, lost_dest, 
	  count(distinct xref_ims_pat_nbr) as pat_cnt, 
	  count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 
group by 1,2,3,4,5 
order by 1,2,3,4,5
limit 500;
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, 
	  count(distinct xref_ims_pat_nbr) as pat_cnt, 
	  count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 
group by 1,2,3,4 
order by 1,2,3,4
limit 250;
select distinct otlt_chnl_ind, lost_dest from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3
;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 
--where lost_dest="Off Panel" 
limit 30
;


/*****************************************/ 
--assign responsible lost destination
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3 stored as parquet as
select 
	usc_3_cd,usc_3_desc,cohort_month, cohort_ims_yyyymm_dspnsd_dt,cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, lost_dest
from
	(
	select 
		usc_3_cd,usc_3_desc, cohort_month, cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, lost_dest, adj_rx_cnt_per_dest, 
		row_number () over (partition by usc_3_cd,usc_3_desc,cohort_month, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, 
		after_6mth_gap_end, lost_month order by adj_rx_cnt_per_dest desc, max_date desc) as rowno
	from 
		(
		select 
			usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, 
			lost_dest, count(1) as rx_cnt_per_dest, sum(rxs_90dyadj) as adj_rx_cnt_per_dest, max(ims_dspnsd_dt) as max_date
		from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3
		group by 1,2,3,4,5,6,7,8,9,10,11
		) a 
	)b
where b.rowno=1;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3; --6030565


/*****************************************/ 
--Remove Insulin patients from Diabetes market to make a new cohort
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3 stored as parquet as 
select 
	lst2.*
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 lst2
	left join
		(
		select 
			distinct usc_3_cd,usc_3_desc, cohort_month, cohort_ims_yyyymm_dspnsd_dt, cohort_type, lst.xref_ims_pat_nbr as ins_xref_ims_pat_nbr
		from 
			prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 lst,
			prod_df2_cls.CLS_CUSTM_EXTRACT_TRANS lrx,
			PROD_DF2_SR3_DPT.JBK_INSULINS_MKT_JUL17 mdf
		where lst.usc_3_cd='39200'
			--and lst.lost_month<=12
			and lrx.ims_yyyymm_dspnsd_dt<=201705
			and lst.xref_ims_pat_nbr=lrx.xref_ims_pat_nbr
			and CAST(mdf.NDC AS BIGINT)=CAST(lrx.DSPNSD_NDC AS BIGINT) 
			and lrx.otlt_chnl_ind in ('RETAIL')          
			and lrx.misbridge_pat_flag <> 1
			and lrx.ims_supplier_id <> 725 
			and lrx.ST_CD not in ('VI','PR')
		) ins
	on lst2.usc_3_cd=ins.usc_3_cd
		and lst2.cohort_ims_yyyymm_dspnsd_dt=ins.cohort_ims_yyyymm_dspnsd_dt
		and lst2.cohort_type=ins.cohort_type
		and lst2.xref_ims_pat_nbr=ins.ins_xref_ims_pat_nbr
where ins_xref_ims_pat_nbr is null
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3; --9933471

--QC: Compare before and after remove insulin
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3 
--where lost_dest='Competitor'
group by 1,2,3,4 order by 1,2,3,4
limit 250;
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3 
where lost_month<=12
group by 1,2,3,4 order by 1,2,3,4
limit 250;--5933846
select * from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3 limit 10
;


/*****************************************/ 
--Remove WMT mail ims_supplier_id=348 patients from LOST cohort to make a new cohort
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3 stored as parquet as 
select 
	  lst2.usc_3_cd
	, lst2.usc_3_desc
	, lst2.cohort_month
	, lst2.cohort_ims_yyyymm_dspnsd_dt
	, lst2.cohort_type
	, lst2.xref_ims_pat_nbr
	, lst2.lastrx_before_lost
	, lst2.before_gap_start
	, lst2.after_6mth_gap_end
	, case 
		when lst2.lost_month=13 and wmail.xref_ims_pat_nbr is not null then 14
		when lst2.lost_month<13 and wmail.xref_ims_pat_nbr is not null then 13
		else lst2.lost_month end as lost_month
	, case 
		when lst2.lost_month=13 or (lst2.lost_month<13 and wmail.xref_ims_pat_nbr is not null) then 'not lost'
		else lstd.lost_dest end as lost_dest
from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3 lst2 
	--cohort after removing insulin pats from Diabetes market
	left join
		( 
		select distinct usc_3_cd,usc_3_desc, cohort_month, cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr
		from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 
		where ims_supplier_id=348 --6746
		) wmail 
		--pats who ever used supplier=348 in the look forward period
	on lst2.usc_3_cd=wmail.usc_3_cd
		and lst2.cohort_ims_yyyymm_dspnsd_dt=wmail.cohort_ims_yyyymm_dspnsd_dt
		and lst2.cohort_type=wmail.cohort_type
		and lst2.xref_ims_pat_nbr=wmail.xref_ims_pat_nbr
	left join 
		prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3 lstd 
		--get lost_dest for remaining patients
	on lst2.usc_3_cd=lstd.usc_3_cd
		and lst2.cohort_ims_yyyymm_dspnsd_dt=lstd.cohort_ims_yyyymm_dspnsd_dt
		and lst2.cohort_type=lstd.cohort_type
		and lst2.xref_ims_pat_nbr=lstd.xref_ims_pat_nbr
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3; --9933471

--QC
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt
from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3 
group by 1,2,3,4 order by 1,2,3,4
limit 250;
select lost_month, count(1) from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3 group by 1 order by 1
;
select lost_month, count(1) from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_v3 group by 1 order by 1
;
select lost_month,lost_dest, count(1) from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3 group by 1,2 order by 1,2
;


/*****************************************/ 
/*****************************************/ 
--Assign resp_COT for All Lost patient


/*****************************************/ 
--NEW
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3 stored as parquet as 
with rx as (
			select lfrx.*
			from  prod_df2_sr3_dpt.zz_wmt_fillp_Npat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 lfrx
			inner join
				(
				select * 
				from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3
				where lost_month<=12 
				) pat
			on lfrx.lost_dest=pat.lost_dest
				and lfrx.usc_3_cd=pat.usc_3_cd
				and lfrx.cohort_ims_yyyymm_dspnsd_dt=pat.cohort_ims_yyyymm_dspnsd_dt
				and lfrx.cohort_type=pat.cohort_type
				and lfrx.xref_ims_pat_nbr=pat.xref_ims_pat_nbr
		  )
		  --temp table: get All NEW lost patients' look forward rxs
select usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, lost_dest, cot
from
	(
		select 
		usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, 
		lost_dest,cot, adj_rx_cnt_per_dest, row_number () over (partition by usc_3_cd,usc_3_desc,cohort_month, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, 
		after_6mth_gap_end, lost_month,lost_dest order by adj_rx_cnt_per_dest desc, max_date desc) as rowno
		from 
		(
			select usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, 
			lost_dest,cot, count(1) as rx_cnt_per_dest, sum(rxs_90dyadj) as adj_rx_cnt_per_dest, max(ims_dspnsd_dt) as max_date
			from 
				(
				select *, 
					case
					when org_cd ='033' then 'Loss_Sam'
					when org_cd in ('032','955') then 'Loss_WMT' 
					when lost_dest='Stopped Treatment' and otlt_cot_ind is null then 'Off Grid' 
					when otlt_cot_ind='OTHER' then 'Loss_Mail'
					when otlt_cot_ind is null then 'ATTENTION'
					else concat('Loss_',initcap(otlt_cot_ind)) end as COT
				from rx
				) raw
			group by 1,2,3,4,5,6,7,8,9,10,11,12
		) a 
	)b
where b.rowno=1
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3; --5929554

--QC
select distinct lost_dest, cot from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3
; 
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type,lost_month,lost_dest,cot,count(distinct xref_ims_pat_nbr)  
from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7
limit 300;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3 limit 20
;


/*********************************************************************************************************************************/
-- The following part is for CONTINUE Patients
/*********************************************************************************************************************************/


/*****************************************/ 
--Find CONTINUING pats in the 2015 Top 10 USC3 maint_non_spcl cohorts by looking at 12 lookback periods
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3 stored as parquet as
select 
	cht.*
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_pat_status_T10USC3_cohorts_2015_maint_non_spcl_v3 cht
where pat_status = 'CONTINUING'
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3;--137717711

--QC on obs rxs
select usc_3_cd,usc_3_desc,cohort_month,org_cd, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3 group by 1,2,3,4 order by 1,2,3,4 
limit 250;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3 limit 10
;
--QC
select usc_3_cd,usc_3_desc,cohort_month,org_cd, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl
group by 1,2,3,4 
order by 1,2,3,4; 

select usc_3_cd,usc_3_desc,cohort_month,org_cd, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3 
group by 1,2,3,4 
order by 1,2,3,4; 


/*****************************************/ 
--Extract RXS for cohort pats (CONTINUING 2015 top 10 USC3 maint_non_spcl);
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3 stored as parquet as
select 
	rx.*
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_Allpat_rxs_T10USC3_cohorts_2015_maint_non_spcl rx, 
	prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3 cht 
where rx.xref_ims_pat_nbr = cht.xref_ims_pat_nbr 
	and rx.org_cd = cht.org_cd 
	and rx.usc_3_cd = cht.usc_3_cd 
	and rx.cohort_month = cht.cohort_month
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3; --1558834320

--QC on obs rxs
select usc_3_cd,usc_3_desc,cohort_month,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3 group by 1,2,3,4 order by 1,2,3,4 
limit 250;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3 limit 10
;


/*****************************************/ 
--Define 6 months gap for two cohort Patients
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_cont_pat_gaps_T10USC3_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_cont_pat_gaps_T10USC3_2015_maint_non_spcl_v3 stored as parquet as 
select 
	  *
	, CAST(concat(CAST(extract(year from next_Fistof_month_2) AS string),lpad(CAST(extract(month from next_Fistof_month_2) AS string),2,'0')) AS int) as gap_end
	, months_between(next_Fistof_month_2,Fistof_month) as month_gap
from 
	(
	select 
		  chtrx.*
		, lag(chtrx.Fistof_month) over (partition by chtrx.usc_3_cd, chtrx.usc_3_desc ,chtrx.cohort_month, chtrx.cohort_ims_yyyymm_dspnsd_dt, chtrx.cohort_type, chtrx.xref_ims_pat_nbr 
								order by chtrx.gap_start desc) as next_Fistof_month
		, case when lag(chtrx.Fistof_month) over (partition by chtrx.usc_3_cd, chtrx.usc_3_desc ,chtrx.cohort_month, chtrx.cohort_ims_yyyymm_dspnsd_dt, chtrx.cohort_type, chtrx.xref_ims_pat_nbr 
								order by chtrx.gap_start desc) is null then add_months(cohort_month,18) 
			else lag(chtrx.Fistof_month) over (partition by chtrx.usc_3_cd, chtrx.usc_3_desc ,chtrx.cohort_month, chtrx.cohort_ims_yyyymm_dspnsd_dt, chtrx.cohort_type, chtrx.xref_ims_pat_nbr 
								order by chtrx.gap_start desc)
			end as next_Fistof_month_2
	from
		(
		select 
			usc_3_cd
			,usc_3_desc 
			,cohort_month
			,cohort_ims_yyyymm_dspnsd_dt
			,cohort_type
			,xref_ims_pat_nbr
			,Fistof_month
			,ims_yyyymm_dspnsd_dt as gap_start
			,count(1) as rx_cnt
			,sum(Rxs_90dyAdj) as adj_rx_cnt
		from 
			prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_T10USC3_cohorts_2015_maint_non_spcl_v3
		group by 1,2,3,4,5,6,7,8
		) chtrx
	) lag
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pat_gaps_T10USC3_2015_maint_non_spcl_v3 ; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pat_gaps_T10USC3_2015_maint_non_spcl_v3 ; --70537228 cont:1341393187 /1339524005

--QC
select * from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pat_gaps_T10USC3_2015_maint_non_spcl_v3 order by 1,2,3,4 limit 30
;
select usc_3_cd,usc_3_desc, cohort_ims_yyyymm_dspnsd_dt, cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt, sum(rx_cnt) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pat_gaps_T10USC3_2015_maint_non_spcl_v3 group by 1,2,3,4 order by 1,2,3,4
limit 250; 


/*****************************************/ 
--select new patients nearest 6 month gap
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 stored as parquet as
select 
	cht.usc_3_cd
	, cht.usc_3_desc 
	, cht.cohort_month
	, cht.cohort_ims_yyyymm_dspnsd_dt
	, cht.org_cd as cohort_type 
	, cht.xref_ims_pat_nbr
	, gap.lastrx_before_lost
	, gap.before_gap_start
	, gap.after_6mth_gap_end
	, case when gap.lastrx_before_lost is null then 13
		else months_between(gap.lastrx_before_lost,cht.cohort_month)+1 
		end as lost_month
from 
	(
	select 
	  usc_3_cd
	, usc_3_desc
	, cohort_month
	, cohort_ims_yyyymm_dspnsd_dt
	, cohort_type
	, xref_ims_pat_nbr
	, min(Fistof_month) as lastrx_before_lost
	, min(gap_start) as before_gap_start
	, CAST(concat(CAST(extract(year from add_months(min(Fistof_month),7)) AS string),lpad(CAST(extract(month from add_months(min(Fistof_month),7)) AS string),2,'0')) AS int) as after_6mth_gap_end
	from 
		prod_df2_sr3_dpt.zz_wmt_fillp_cont_pat_gaps_T10USC3_2015_maint_non_spcl_v3 
	where month_gap>=7 
	group by 1,2,3,4,5,6
	) gap 
right join 
	prod_df2_sr3_dpt.zz_wmt_fillp_pat_cont_T10USC3_cohorts_2015_maint_non_spcl_v3 cht
on gap.usc_3_cd=cht.usc_3_cd
	and gap.xref_ims_pat_nbr=cht.xref_ims_pat_nbr
	and gap.cohort_type=cht.org_cd
	and gap.cohort_month=cht.cohort_month
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3; --14643034 cont145094370 /137717711

--QC
select * from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 limit 10
;
select usc_3_cd, usc_3_desc,cohort_ims_yyyymm_dspnsd_dt, cohort_type, lost_month, count(1) as pat_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 group by 1,2,3,4,5 order by 1,2,3,4,5
limit 100;


/*****************************************/ 
--We will find lost patients' lost DESTINATION directly in this program
/*****************************************/ 
--Extract Lost patients' rxs in 6 months LF period
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 stored as parquet as 
select 
	 pats.usc_3_cd
	,pats.usc_3_desc
	,pats.cohort_month
	,pats.cohort_ims_yyyymm_dspnsd_dt
	,pats.cohort_type
	,pats.xref_ims_pat_nbr
	,pats.lastrx_before_lost
	,pats.before_gap_start
	,pats.after_6mth_gap_end
	,pats.lost_month
	,raw.usc_cd
	,raw.ims_yyyymm_dspnsd_dt
	,raw.org_cd
	,raw.otlt_chnl_ind
	,case when raw.otlt_chnl_ind='MAIL' then 'Mail'
		when pats.cohort_type!=raw.org_cd and raw.otlt_chnl_ind='RETAIL' then 'Competitor'
		when pats.cohort_type=raw.org_cd then 'ATTENTION'
		when raw.org_cd is null then 'Stopped Treatment'
		end as lost_dest
	,case when raw.ddsupp_cnt < 84 then 1   /** adjust 90 day RX ******/
		else 3 end  as Rxs_90dyAdj
	,raw.ims_dspnsd_dt
	,raw.maint_cmf10
	,raw.st_cd
	,raw.otlt_cot_ind
	,raw.supplier_par_id
	,raw.otlt_ims_id
	,raw.rx_qty
	,raw.pay_typ_cd
	,raw.pat_gender_cd
	,raw.pat_age_nbr
	,raw.ims_supplier_id
	,raw.ddsupp_cnt
	,raw.dspnsd_ndc
	,raw.cmf10
	,raw.prod_desc
	,raw.mp_surro_id
	,raw.mp_ims_id
	,raw.mp_seq_ims_id
	,raw.payer_pln_surro_id
	,raw.payer_pln_curr_xref
	,raw.mdel_typ_curr
	,raw.claim_id
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 pats
left join 
	( 
	select 
		lrx.*,prod.usc_3_cd, prod.prod_desc
	from 
		prod_df2_cls.CLS_CUSTM_EXTRACT_TRANS lrx,
		prod_df2_sr3_dpt.xr_wmt_lpp_top10_usc_mdf prod
	WHERE lrx.cmf10=prod.cmf10
		and lrx.ims_yyyymm_dspnsd_dt between 201501 and 201705
		and lrx.otlt_chnl_ind in ('RETAIL','MAIL')          
		and lrx.misbridge_pat_flag <> 1
		and lrx.ims_supplier_id <> 725 
		and lrx.ST_CD not in ('VI','PR')
		and lrx.maint_cmf10=1
	) raw
on raw.usc_3_cd=pats.usc_3_cd
	and raw.xref_ims_pat_nbr=pats.xref_ims_pat_nbr
	and pats.before_gap_start<raw.ims_yyyymm_dspnsd_dt
	and pats.after_6mth_gap_end>raw.ims_yyyymm_dspnsd_dt
WHERE pats.lost_month<13
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3; --15511959 cont86819180 /74624361

--QC
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, lost_dest, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 group by 1,2,3,4,5 order by 1,2,3,4,5
limit 500;
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt, count(1) as rx_cnt 
from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 group by 1,2,3,4 order by 1,2,3,4
limit 250;
select distinct otlt_chnl_ind, lost_dest from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 
;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3  
where ims_yyyymm_dspnsd_dt <=before_gap_start or ims_yyyymm_dspnsd_dt>=after_6mth_gap_end limit 20
;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 
--where lost_dest="Off Panel" 
limit 30
;


/*****************************************/ 
--assign responsible lost destination
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3;
create table prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3 stored as parquet as
select 
	usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt,
	cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, lost_dest
from
	(
	select 
		usc_3_cd,usc_3_desc, cohort_month, cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, 
		lost_dest, adj_rx_cnt_per_dest, row_number () over (partition by usc_3_cd,usc_3_desc,cohort_month, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, 
		after_6mth_gap_end, lost_month order by adj_rx_cnt_per_dest desc, max_date desc) as rowno
	from 
		(
		select 
			usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, 
			lost_dest, count(1) as rx_cnt_per_dest, sum(rxs_90dyadj) as adj_rx_cnt_per_dest, max(ims_dspnsd_dt) as max_date
		from 
			prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3
		group by 1,2,3,4,5,6,7,8,9,10,11
		) a 
	)b
where b.rowno=1;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3;
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3; --10489533 cont48207127 /40509613


/*****************************************/ 
--Remove Insulin patients from Diabetes market to make a new cohort
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3 stored as parquet as 
select 
	lst2.*
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 lst2
	left join
		(
		select 
			distinct usc_3_cd,usc_3_desc, cohort_month, cohort_ims_yyyymm_dspnsd_dt, cohort_type, lst.xref_ims_pat_nbr as ins_xref_ims_pat_nbr
		from 
			prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 lst,
			prod_df2_cls.CLS_CUSTM_EXTRACT_TRANS lrx,
			PROD_DF2_SR3_DPT.JBK_INSULINS_MKT_JUL17 mdf
		where lst.usc_3_cd='39200'
			--and lst.lost_month<=12
			and lrx.ims_yyyymm_dspnsd_dt<=201705
			and lst.xref_ims_pat_nbr=lrx.xref_ims_pat_nbr
			and CAST(mdf.NDC AS BIGINT)=CAST(lrx.DSPNSD_NDC AS BIGINT) 
			and lrx.otlt_chnl_ind in ('RETAIL')          
			and lrx.misbridge_pat_flag <> 1
			and lrx.ims_supplier_id <> 725 
			and lrx.ST_CD not in ('VI','PR')
		) ins --Patients who ever used insulin
	on lst2.usc_3_cd=ins.usc_3_cd
		and lst2.cohort_ims_yyyymm_dspnsd_dt=ins.cohort_ims_yyyymm_dspnsd_dt
		and lst2.cohort_type=ins.cohort_type
		and lst2.xref_ims_pat_nbr=ins.ins_xref_ims_pat_nbr
where ins_xref_ims_pat_nbr is null
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3; --134083208

--QC: Compare before and after remove insulin
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt,sum (count(distinct xref_ims_pat_nbr)) over ()
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_6month_gap_T10USC3_2015_maint_non_spcl_v3 
group by 1,2,3,4 order by 1,2,3,4
limit 250;
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt,sum (count(distinct xref_ims_pat_nbr)) over ()
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3 
group by 1,2,3,4 order by 1,2,3,4
limit 250;
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt, sum (count(distinct xref_ims_pat_nbr)) over ()
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3 
where lost_month<=12
group by 1,2,3,4 order by 1,2,3,4
limit 250;--39473118


/*****************************************/ 
--Remove WMT mail ims_supplier_id=348 patients from LOST cohort to make a new cohort
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3 stored as parquet as 
select 
	  lst2.usc_3_cd
	, lst2.usc_3_desc
	, lst2.cohort_month
	, lst2.cohort_ims_yyyymm_dspnsd_dt
	, lst2.cohort_type
	, lst2.xref_ims_pat_nbr
	, lst2.lastrx_before_lost
	, lst2.before_gap_start
	, lst2.after_6mth_gap_end
	, case 
		when lst2.lost_month=13 and wmail.xref_ims_pat_nbr is not null then 14
		when lst2.lost_month<13 and wmail.xref_ims_pat_nbr is not null then 13
		else lst2.lost_month end as lost_month
	, case 
		when lst2.lost_month=13 or (lst2.lost_month<13 and wmail.xref_ims_pat_nbr is not null) then 'not lost'
		else lstd.lost_dest end as lost_dest
from 
	prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3 lst2 
	--cohort after removing insulin pats from Diabetes market
	left join
		( 
		select 
			distinct usc_3_cd,usc_3_desc, cohort_month, cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr
		from 
			prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 
		where ims_supplier_id=348 
		) wmail  
		--pats who ever used supplier=348 in the look forward period
	on lst2.usc_3_cd=wmail.usc_3_cd
		and lst2.cohort_ims_yyyymm_dspnsd_dt=wmail.cohort_ims_yyyymm_dspnsd_dt
		and lst2.cohort_type=wmail.cohort_type
		and lst2.xref_ims_pat_nbr=wmail.xref_ims_pat_nbr
	left join 
		prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_lost_dest_T10USC3_2015_maint_non_spcl_v3 lstd 
		--get lost_dest for remaining patients
	on lst2.usc_3_cd=lstd.usc_3_cd
		and lst2.cohort_ims_yyyymm_dspnsd_dt=lstd.cohort_ims_yyyymm_dspnsd_dt
		and lst2.cohort_type=lstd.cohort_type
		and lst2.xref_ims_pat_nbr=lstd.xref_ims_pat_nbr
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3; --134083208


--QC
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3 
group by 1,2,3,4 order by 1,2,3,4
limit 250;
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type, count(distinct xref_ims_pat_nbr) as pat_cnt
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3 
where lost_month<=12
group by 1,2,3,4 order by 1,2,3,4
limit 250;
select lost_month, count(1) from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_v3 group by 1 order by 1
;
select lost_month, count(1) from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3 group by 1 order by 1
;
select lost_month,lost_dest, count(1) from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3 group by 1,2 order by 1,2
;


/*****************************************/ 
/*****************************************/ 
--Assign resp_COT for All Lost patient


/*****************************************/ 
--CONTINUING
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3 stored as parquet as 
with rx as (
			select lfrx.*
			from  prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_rxs_6month_lost_T10USC3_2015_maint_non_spcl_v3 lfrx
			inner join
				(
				select * 
				from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3
				where lost_month<=12 
				) pat
			on lfrx.lost_dest=pat.lost_dest
				and lfrx.usc_3_cd=pat.usc_3_cd
				and lfrx.cohort_ims_yyyymm_dspnsd_dt=pat.cohort_ims_yyyymm_dspnsd_dt
				and lfrx.cohort_type=pat.cohort_type
				and lfrx.xref_ims_pat_nbr=pat.xref_ims_pat_nbr
		  )
		  --temp table: get All CONTINUING lost patients' look forward rxs
select usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, lost_dest, cot
from
	(
		select 
		usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, 
		lost_dest,cot, adj_rx_cnt_per_dest, row_number () over (partition by usc_3_cd,usc_3_desc,cohort_month, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, 
		after_6mth_gap_end, lost_month,lost_dest order by adj_rx_cnt_per_dest desc, max_date desc) as rowno
		from 
		(
			select usc_3_cd,usc_3_desc,cohort_month,cohort_ims_yyyymm_dspnsd_dt, cohort_type, xref_ims_pat_nbr,lastrx_before_lost, before_gap_start, after_6mth_gap_end, lost_month, 
			lost_dest,cot, count(1) as rx_cnt_per_dest, sum(rxs_90dyadj) as adj_rx_cnt_per_dest, max(ims_dspnsd_dt) as max_date
			from 
				(
				select *, 
					case
					when org_cd ='033' then 'Loss_Sam'
					when org_cd in ('032','955') then 'Loss_WMT' 
					when lost_dest='Stopped Treatment' and otlt_cot_ind is null then 'Off Grid' 
					when otlt_cot_ind='OTHER' then 'Loss_Mail'
					when otlt_cot_ind is null then 'ATTENTION'
					else concat('Loss_',initcap(otlt_cot_ind)) end as COT
				from rx
				) raw
			group by 1,2,3,4,5,6,7,8,9,10,11,12
		) a 
	)b
where b.rowno=1
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3; --39445419


--QC
select distinct lost_dest, cot from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3
; 
select usc_3_cd,usc_3_desc,cohort_ims_yyyymm_dspnsd_dt,cohort_type,lost_month,lost_dest,cot,count(distinct xref_ims_pat_nbr)  
from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3
group by 1,2,3,4,5,6,7
order by 1,2,3,4,5,6,7
limit 300;
select * from prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3 limit 20
;


/*****************************************/ 
/*****************************************/ 
--Survival Curve


/*****************************************/ 
--Make Survival Curve Separately for NEW and CONTINUING
--NEW
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2 stored as parquet as 
with lost as (
				select 
					usc_3_cd
					,usc_3_desc
					,cohort_type as type 
					,cohort_ims_yyyymm_dspnsd_dt as cohort_month
					,lost_month
					,lost_dest_new as lost_dest
					,cot as lost_dest_detail
					--,pat_value_ind_1,psycho_antipsycho,psycho_antidepress,psycho_antimania,psycho_analeptics,psycho_antianxiety,psycho_newgen,psycho_other
					,count(distinct xref_ims_pat_nbr) as lost_dest_pat_cnt
				from 
					(
						select 
							 *
							,case when lost_dest='Mail' then 'Lost to Mail' 
								when lost_dest='Competitor' then 'Lost to Competitor' 
								when lost_dest='Stopped Treatment' then 'Off Grid' 
								else 'ATTENTION' 
								end as lost_dest_new
						from 
							prod_df2_sr3_dpt.zz_wmt_fillp_Npat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3
					) a
				group by 1,2,3,4,5,6,7
			 ),
			 --Temp table: USC3/cohort_type/cohort_month/lost_month/lost_dest/cot level patient count
	alst as (
				select usc_3_cd,
					usc_3_desc,
					type
					,cohort_ims_yyyymm_dspnsd_dt as cohort_month
					,lost_month
					,survival as cohort_tot
					,survival-lost as survival
					,(survival-lost)/total as Rate
				from 
					(
						select 
							usc_3_cd,
							usc_3_desc,
							cohort_type as type, 
							cohort_ims_yyyymm_dspnsd_dt,
							lost_month,
							count(distinct xref_ims_pat_nbr) as lost,
							sum(count(distinct xref_ims_pat_nbr)) over (partition by usc_3_cd,usc_3_desc,cohort_type,cohort_ims_yyyymm_dspnsd_dt order by lost_month desc rows between unbounded preceding and current row) as survival,
							sum(count(distinct xref_ims_pat_nbr)) over (partition by usc_3_cd,usc_3_desc,cohort_type,cohort_ims_yyyymm_dspnsd_dt) as total
						from 
							prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_cohort_noinsl_Wmail_cln_v3
						group by 1,2,3,4,5
					) rpt
				where lost_month<=12
			)
			--Temp table: USC3/cohort_type/cohort_month/lost_month level patient count
select case when rpt2.usc_3_cd='31100' then 1  
		  when rpt2.usc_3_cd='32100' then 5  
		  when rpt2.usc_3_cd='31400' then 2  
		  when rpt2.usc_3_cd='39200' then 3  
		  when rpt2.usc_3_cd='64300' then 4  
		  when rpt2.usc_3_cd='72100' then 6  
		  when rpt2.usc_3_cd='41100' then 7  
		  when rpt2.usc_3_cd='23400' then 8  
		  when rpt2.usc_3_cd='31300' then 9  
		  when rpt2.usc_3_cd='20200' then 10 
		  else null 
	   end as rank
	   ,rpt2.usc_3_cd
	   ,rpt2.usc_3_desc
	   ,'NEW' as pat_status
	   ,rpt2.cohort_month
	   ,case when rpt2.type='032' then 'Supercenter' 
	         when rpt2.type='955' then 'Neighborhood' 
			 else 'null' 
			end as Type
	   ,rpt2.lost_month
	   ,rpt2.cohort_tot as cohort
	   ,lst.lost_dest
	   ,lst.lost_dest_detail
	   ,lst.lost_dest_pat_cnt as lost
	   ,rpt2.survival
	   ,rpt2.Rate
from 
	(
		select * from lost
		Union all
		select usc_3_cd,usc_3_desc,type, cohort_month,0 as lost_month,lost_dest,lost_dest_detail ,0 as lost_dest_pat_cnt  from lost where lost_month=1
	) lst 
	inner join 
	(
		SELECT * from alst
		union all
		select usc_3_cd,usc_3_desc,type, cohort_month,0 as lost_month,cohort_tot, cohort_tot as survival, 1 as rate from alst where lost_month=1
	) rpt2
	on rpt2.usc_3_cd=lst.usc_3_cd
		and rpt2.type=lst.type 
		and rpt2.cohort_month=lst.cohort_month 
		and rpt2.lost_month=lst.lost_month;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2; --24385


--QC
select * from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2 order by 1,2,3,4 desc,5,6 desc,7,8,9,10 limit 300
;
select usc_3_cd,cohort_month,type,count(distinct lost_dest_detail),count(1)
from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2 where lost_month=0 and lost_dest='Lost to Competitor'
group by 1,2,3
order by 1,2,3
;
select distinct lost_dest,lost_dest_detail from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2
;
select usc_3_cd,cohort_month,type,sum(lost) from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2 group by 1,2,3 order by 1,2,3 desc
;
select usc_3_cd,cohort_month,type,max(cohort) from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2 group by 1,2,3 order by 1,2,3 desc
;
select usc_3_cd,cohort_month,type,sum(lost) from prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt group by 1,2,3 order by 1,2,3 desc
;


--CONTINUING
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2 stored as parquet as 
with lost as (
				select 
					usc_3_cd
					,usc_3_desc
					,cohort_type as type 
					,cohort_ims_yyyymm_dspnsd_dt as cohort_month
					,lost_month
					,lost_dest_new as lost_dest
					,cot as lost_dest_detail
					--,pat_value_ind_1,psycho_antipsycho,psycho_antidepress,psycho_antimania,psycho_analeptics,psycho_antianxiety,psycho_newgen,psycho_other
					,count(distinct xref_ims_pat_nbr) as lost_dest_pat_cnt
				from 
					(
						select 
							 *
							,case when lost_dest='Mail' then 'Lost to Mail' 
								when lost_dest='Competitor' then 'Lost to Competitor' 
								when lost_dest='Stopped Treatment' then 'Off Grid' 
								else 'ATTENTION' 
								end as lost_dest_new
						from 
							prod_df2_sr3_dpt.zz_wmt_fillp_Cpat_aflost_resp_cot_T10USC3_2015_maint_non_spcl_v3
					) a
				group by 1,2,3,4,5,6,7
			 ),
			 --Temp table: USC3/cohort_type/cohort_month/lost_month/lost_dest/cot level patient count
	alst as (
				select usc_3_cd,
					usc_3_desc,
					type
					,cohort_ims_yyyymm_dspnsd_dt as cohort_month
					,lost_month
					,survival as cohort_tot
					,survival-lost as survival
					,(survival-lost)/total as Rate
				from 
					(
						select 
							usc_3_cd,
							usc_3_desc,
							cohort_type as type, 
							cohort_ims_yyyymm_dspnsd_dt,
							lost_month,
							count(distinct xref_ims_pat_nbr) as lost,
							sum(count(distinct xref_ims_pat_nbr)) over (partition by usc_3_cd,usc_3_desc,cohort_type,cohort_ims_yyyymm_dspnsd_dt order by lost_month desc rows between unbounded preceding and current row) as survival,
							sum(count(distinct xref_ims_pat_nbr)) over (partition by usc_3_cd,usc_3_desc,cohort_type,cohort_ims_yyyymm_dspnsd_dt) as total
						from 
							prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_cohort_noinsl_Wmail_cln_v3
						group by 1,2,3,4,5
					) rpt
				where lost_month<=12
			)
			--Temp table: USC3/cohort_type/cohort_month/lost_month level patient count
select 
	case when rpt2.usc_3_cd='31100' then 1  
		 when rpt2.usc_3_cd='32100' then 5  
		 when rpt2.usc_3_cd='31400' then 2  
		 when rpt2.usc_3_cd='39200' then 3  
		 when rpt2.usc_3_cd='64300' then 4  
		 when rpt2.usc_3_cd='72100' then 6  
		 when rpt2.usc_3_cd='41100' then 7  
		 when rpt2.usc_3_cd='23400' then 8  
		 when rpt2.usc_3_cd='31300' then 9  
		 when rpt2.usc_3_cd='20200' then 10 
		 else null 
		 end as rank
		 ,rpt2.usc_3_cd
		 ,rpt2.usc_3_desc
		 ,'CONTINUING' as pat_status
		 ,rpt2.cohort_month
		 ,case when rpt2.type='032' then 'Supercenter' 
			   when rpt2.type='955' then 'Neighborhood' 
			   else 'null'  end as Type
		 ,rpt2.lost_month
		 ,rpt2.cohort_tot as cohort
		 ,lst.lost_dest
		 ,lst.lost_dest_detail
		 ,lst.lost_dest_pat_cnt as lost
		 ,rpt2.survival
		 ,rpt2.Rate
from 
	(
		select * from lost
		Union all
		select usc_3_cd,usc_3_desc,type, cohort_month,0 as lost_month,lost_dest,lost_dest_detail ,0 as lost_dest_pat_cnt  from lost where lost_month=1
	) lst 
	inner join 
	(
		SELECT * from alst
		union all
		select usc_3_cd,usc_3_desc,type, cohort_month,0 as lost_month,cohort_tot, cohort_tot as survival, 1 as rate from alst where lost_month=1
	) rpt2
	on rpt2.usc_3_cd=lst.usc_3_cd
		and rpt2.type=lst.type 
		and rpt2.cohort_month=lst.cohort_month 
		and rpt2.lost_month=lst.lost_month;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2; --24958

--QC
select * from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2 order by 1,2,3,4 desc,5,6 desc,7,8,9,10 limit 10
;
select usc_3_cd,cohort_month,type,count(distinct lost_dest_detail),count(1)
from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2 where lost_month=0 and lost_dest='Lost to Competitor'
group by 1,2,3
order by 1,2,3
;
select distinct lost_dest,lost_dest_detail from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2
;
select usc_3_cd,cohort_month,type,sum(lost) from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2 group by 1,2,3 order by 1,2,3 desc
;
select usc_3_cd,cohort_month,type,max(cohort) from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2 group by 1,2,3 order by 1,2,3 desc
;
select usc_3_cd,cohort_month,type,sum(lost) from prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt group by 1,2,3 order by 1,2,3 desc
;


/*****************************************/ 
--Combine NEW and CONTINUING Survial Curve together
drop table if exists prod_df2_sr3_dpt.zz_wmt_fillp_All_pats_Survial_Curve_v3_updt2; 
create table prod_df2_sr3_dpt.zz_wmt_fillp_All_pats_Survial_Curve_v3_updt2 stored as parquet as 
select * from  prod_df2_sr3_dpt.zz_wmt_fillp_new_pats_Survial_Curve_v3_updt2
union all
select * from  prod_df2_sr3_dpt.zz_wmt_fillp_cont_pats_Survial_Curve_v3_updt2
;
compute stats prod_df2_sr3_dpt.zz_wmt_fillp_All_pats_Survial_Curve_v3_updt2; 
show table stats prod_df2_sr3_dpt.zz_wmt_fillp_All_pats_Survial_Curve_v3_updt2; --49343

--QC
select * from prod_df2_sr3_dpt.zz_wmt_fillp_All_pats_Survial_Curve_v3_updt2 order by 1,2,3,4 desc,5,6 desc,7,8,9,10 limit 200
;
select rank,usc_3_cd,usc_3_desc,pat_status,cohort_month,type,lost_month, lost_dest,lost_dest_detail, max(cohort),sum(lost),max(rate)  
from prod_df2_sr3_dpt.zz_wmt_fillp_All_pats_Survial_Curve_v3_updt where lost_month>0 group by 1,2,3,4,5,6,7,8,9 order by 1,2,3,4 desc,5,6 desc,7,8,9,10 limit 200
;