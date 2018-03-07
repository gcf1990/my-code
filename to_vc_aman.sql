-------------
drop table gcf_test1;
create table gcf_test1 as
select  customer_id, cast((3 * length(replace(total_people, '+', '')) - length(replace(total_people, '+', ''))) / 2 as bigint) leng
from owo_ods.kylin_customer__ls_customer_customer_details ;

--------------
drop table gcf_customer_total_people;
create table gcf_customer_total_people as
select cd.customer_id, regexp_extract(substr(cd.total_people, 1, t.leng),'([0-9]+)',1) total_people 
    from owo_ods.kylin_customer__ls_customer_customer_details cd
    inner join gcf_test1  t on cd.customer_id=t.customer_id and t.leng>0
 union all
select
cd.customer_id, '' as total_people 
    from owo_ods.kylin_customer__ls_customer_customer_details cd
    inner join (select * from gcf_test1 where leng=0 or leng is null) t on cd.customer_id=t.customer_id


---客户人数分部
select
b.total_people,
sum(machine_qyt) machine_qyt,
count(distinct b.customer_id) customer_qyt
from
(
  select c.customer_id, c.corp_name,
  case when d.total_people is null then 'empty'
	when d.total_people = '' then 'empty'
	when d.total_people < 30 then '30-' 
	when d.total_people >= 30 and d.total_people < 50 then '30-50'
	when d.total_people >= 50 and d.total_people < 100 then '50-100'
	when d.total_people >= 100 and d.total_people < 300 then '100-300'
	when d.total_people >= 300 then '300+'
	else 'fuck' end as total_people, 
  count(r.machine_id)  machine_qyt
  from 
  (
  select  machine_id,customer_id
  from rack.dw_machine where to_char(online_time,'yyyymmdd')>=20171201 and to_char(online_time,'yyyymmdd')<20180101
  )r 
  join rack.dw_customer c on r.customer_id=c.customer_id
  join
  (
    select cd.customer_id, regexp_extract(substr(cd.total_people, 1, t.leng),'([0-9]+)',1) total_people 
    from owo_ods.kylin_customer__ls_customer_customer_details cd
    inner join gcf_test1  t on cd.customer_id=t.customer_id and t.leng>0
  )d on d.customer_id=c.customer_id
  group by 
  c.customer_id, c.corp_name,
  case when d.total_people is null then 'empty'
	when d.total_people = '' then 'empty'
	when d.total_people < 30 then '30-' 
	when d.total_people >= 30 and d.total_people < 50 then '30-50'
	when d.total_people >= 50 and d.total_people < 100 then '50-100'
	when d.total_people >= 100 and d.total_people < 300 then '100-300'
	when d.total_people >= 300 then '300+'
	else 'fuck' end
)b
group by
b.total_people
；


---累计扫码人数
select 
b.mon,
sum(b.user_qyt) over (order by b.mon asc)sum_user
from 
(
select 
substr(a.create_time,1,7) mon,
count(distinct a.user_id)user_qyt
from 
(
select 
dd.create_time,
coalesce(dd.token, dd.user_id) token,
at.user_id,
row_number() over(partition by at.user_id order by dd.create_time asc) rn
from owo_ods.h_com_stat__userevent dd
inner join owo_ods.kylin_account__ls_account_token at on dd.token=at.token
where app_name= 'app-openrack-h5' and  page_name= 'commodity_list' and  log_type= 'pv'
)a
where a.rn=1
group  by
substr(a.create_time,1,7)
)b
;


------50人以下（包括人数为空）公司
create table gcf_customer_total_people_50 as
select *
from
(
select c.customer_id, c.corp_name,d.total_people,
count(r.machine_id)  machine_qyt
from 
  (
  select  machine_id,customer_id
  from rack.dw_machine where release_status in(2,3)
  )r 
join rack.dw_customer c on r.customer_id=c.customer_id
join
  (
    select cd.customer_id, regexp_extract(substr(cd.total_people, 1, t.leng),'([0-9]+)',1) total_people 
    from owo_ods.kylin_customer__ls_customer_customer_details cd
    inner join gcf_test1  t on cd.customer_id=t.customer_id and t.leng>0
  )d on d.customer_id=c.customer_id
where  d.total_people < 50
group by 
c.customer_id, c.corp_name,d.total_people

union all


select
c.customer_id, c.corp_name,cd.total_people,
count(r.machine_id)  machine_qyt
from owo_ods.kylin_customer__ls_customer_customer_details cd
inner join rack.dw_customer c on cd.customer_id=c.customer_id
inner join 
(
  select  machine_id,customer_id
  from rack.dw_machine where release_status in(2,3)
)r on r.customer_id=c.customer_id
where cd.total_people is null or cd.total_people=' '
group by
c.customer_id, c.corp_name,cd.total_people
)
;

----公司销售数据
select
o.city_name,
a.customer_id,
a.corp_name,
a.total_people,
m.machine_name,
m.machine_type_name,
m.machine_sub_type_name,
o.weekno,
o.order_qyt
from gcf_customer_total_people_50 a
inner join rack.dw_customer c on a.customer_id=c.customer_id
inner join rack.dw_machine m on c.customer_id=m.customer_id
inner join 
(
	select
	a.*
	from 	
		(
		select
		city_name,
		machine_id,
		weekofyear(pay_complete_time) weekno,
		count(distinct order_id) order_qyt,
		sum(origin_amount) ori 
		from 
		rack.dw_order 
		where pay_status=64
		group by
		city_name,
		machine_id,
		weekofyear(pay_complete_time)
		)a
)o on o.machine_id=m.machine_id
;

-----分级货架分布

select
o.city_name,
case when d.total_people is null then 'empty'
	when d.total_people = '' then 'empty'
	when d.total_people < 30 then '30-' 
	when d.total_people >= 30 and d.total_people < 50 then '30-50'
	when d.total_people >= 50 and d.total_people < 100 then '50-100'
	when d.total_people >= 100 and d.total_people < 300 then '100-300'
	when d.total_people >= 300 then '300+'
	else 'fuck' end as total_people, 
m.machine_id,
g.machine_grading,
m.machine_sub_type_name,
o.order_qyt

from gcf_customer_total_people d 
inner join rack.dwd_rack_customer c on d.customer_id=c.customer_id and c.pt='${bdp.system.bizdate}'
inner join rack.dwd_rack_machine m on c.customer_id=m.customer_id and m.pt='${bdp.system.bizdate}'
inner join  rack.dws_machine_grading g on g.machine_id=m.machine_id and g.pt='${bdp.system.bizdate}'
inner join 
(
	select
	a.city_name,
	a.machine_id,
	avg(order_qyt) order_qyt
	from 	
		(
		select
		city_name,
		machine_id,
		weekofyear(pay_complete_time) weekno,
		count(distinct order_id) order_qyt,
		sum(origin_amount) ori 
		from 
		rack.dwd_rack_order  
		where pay_status=64 and pt='{bdp.system.bizdate}'
		group by
		city_name,
		machine_id,
		weekofyear(pay_complete_time)
		)a
	group by
	a.city_name,
	a.machine_id
)o on o.machine_id=m.machine_id
;

---20180105
---已布点城市数量
select
count(distinct city_name)
from rack.dwd_rack_machine where release_status in(2,3) and pt=20171231
;

---当月新增购买用户数、累计购买用户数、当月购买用户数
---当月新用户
select
count(distinct a.user_id)
from
(
	select
	user_id,
	datepart(min(pay_complete_time),'mm') min_mon
	from rack.dwd_rack_order 
	where pay_status=64 and pt=20171231 

	having datepart(min(pay_complete_time),'mm')=12
)a
;


---工作日订单数
select
sum(a.order_qyt)
from
(
	select
	to_char(pay_complete_time,'yyyymmdd') payday,
	count(distinct order_id) order_qyt
	from rack.dwd_rack_order 
	where pay_status=64 and pt=20171231
	and pay_complete_time>'2017-12-01 00:00:00'
	group by 
	to_char(pay_complete_time,'yyyymmdd')
)a
inner join 
cdm.dim_calendar c on a.payday=c.day_id and c.day_type='工作日'
;

---工作日平均售卖货架数、订单量
select
avg(a.order_mac)
from
(
select
to_char(pay_complete_time,'yyyymmdd') payday,
count(distinct order_id) order_qyt,
count(distinct machine_id) machine_qyt,
count(distinct order_id)/count(distinct machine_id) order_mac
from rack.dwd_rack_order 
where pay_status=64 and pt=20171231
and pay_complete_time>'2017-12-01 00:00:00'
group by 
to_char(pay_complete_time,'yyyymmdd')
)a
inner join 
cdm.dim_calendar c on a.payday=c.day_id and c.day_type='工作日'
;

--- 存货周转天数（满库存pcs/每天售卖pcs）
select -- 满库存pcs
avg(a.max_inventory) max_inventory
from 
(
	select 
	machine_id,
	count(distinct sku_id) sku_qyt,
	sum(max_inventory) max_inventory
	from owo_ods.kylin__machine_commodity_info 
	where is_valid=1
	group by machine_id
)a
;

select -- 单货架每天售卖pcs
avg(b.qyt)
from 
(
	select
	a.machine_id,
	avg(a.qyt) qyt
	from
	(
		select 
		machine_id,
		to_char(pay_complete_time,'yyyymmdd') payday,
		sum(quantity) qyt
		from rack.dwd_rack_order_item 
		where pay_status=64 and pt=20171231
		and pay_complete_time>'2017-12-01 00:00:00'
		group by
		machine_id,
		to_char(pay_complete_time,'yyyymmdd')
	)a
	inner join 
	cdm.dim_calendar c on a.payday=c.day_id and c.day_type='工作日'
	group by a.machine_id
)b
;

-- 截止当月货架sku数
select
count(distinct sku_id) sku_qyt
from owo_ods.kylin__machine_commodity_info_his 
where day_id=20171231 and is_valid=1
;

-- 截止当月供应商数量
select count(distinct supplier_code) from owo_ods.h_scm_commodity__supplier_product_his 
where day_id=20171231 and is_valid=1
;


--- 存货周转天数（以此为准）
select
avg(c.zzdays)
from 
(
	select
	a.machine_id,
	a.max_inventory/b.qyt zzdays
		from
	(
			select 
			machine_id,
			sum(max_inventory) max_inventory
			from owo_ods.kylin__machine_commodity_info 
			where is_valid=1
			group by machine_id
	)a
	inner join 
	(
		select
		a.machine_id,
		avg(a.qyt) qyt
		from
		(
			select 
			machine_id,
			to_char(pay_complete_time,'yyyymmdd') payday,
			sum(quantity) qyt
			from rack.dwd_rack_order_item 
			where pay_status=64 and pt=20171231
			and pay_complete_time>'2017-12-01 00:00:00'
			group by
			machine_id,
			to_char(pay_complete_time,'yyyymmdd')
		)a
		group by a.machine_id
	)b on a.machine_id=b.machine_id
)c
;


--- 每日订单数, 交易额, 动销货架数， 公司数

select 
to_char(o.pay_complete_time,'yyyymmdd') payday,
count(distinct o.order_id) order_qyt,
sum(o.origin_amount) ori,
count(distinct  o.machine_id) machine_qyt,
count(distinct m.customer_id) customer_qyt
from rack.dwd_rack_order o 
inner join rack.dwd_rack_machine m on o.machine_id=m.machine_id and o.pt=20171231 and m.pt=20171231
where o.pay_status=64 and o.pay_complete_time>'2017-11-01 00:00:00'
group by 
to_char(o.pay_complete_time,'yyyymmdd')

--- 热柜订单量
select
to_char(pay_complete_time,'yyyymmdd') payday,
count(distinct o.order_id) order_qyt
from rack.dwd_rack_order o inner join rack.dwd_rack_machine m on o.machine_id=m.machine_id 
and o.pt=20180104 and m.pt=20180104
where o.pay_status=64 and m.machine_sub_type=90
and to_char(pay_complete_time,'yyyymmdd') in(20171129,20171130)
group by
to_char(pay_complete_time,'yyyymmdd')
;

----------------0109
---每周订单量、交易额、客单价
select
to_char(c.monday_date,'yyyymmdd') monday_date,
concat(c.year_name_cn,c.week_of_year,'周') week,
sum(a.order_qyt) order_qyt,
sum(a.ori) ori,
sum(a.ori)/sum(a.order_qyt) 
from
(
select
to_char(pay_complete_time,'yyyymmdd') payday,
count(distinct order_id) order_qyt,
sum(origin_amount) ori,
sum(origin_amount)/count(distinct order_id)
from rack.dwd_rack_order
where pay_status=64 and pt=20180107
group by
to_char(pay_complete_time,'yyyymmdd')
)a
inner join cdm.dim_calendar c on a.payday=c.day_id
group by
to_char(c.monday_date,'yyyymmdd')
concat(c.year_name_cn,c.week_of_year,'周')
;

----货架的地址, 类型, 上线时间, 公司, 公司人数信息
---货架
select
machine_id,
machine_name,
customer_id,
machine_type_name,
machine_sub_type_name,
address,
to_char(online_time,'yyyymmdd') online_date,
case when release_status=-1 and online_time is not null then to_char(update_time,'yyyymmdd')
else '无' end as offline_date,
city_name
from rack.dwd_rack_machine 
where pt=20171231 and release_status in(2,3,-1) and city_name is not null 
;


-- 公司人数，货架数
select
b.customer_id,
case when b.total_people is null or b.total_people='' and b.machine_qyt<4 then '30~50'
when b.total_people is null or b.total_people='' and b.machine_qyt>3 and b.machine_qyt<11 then '100~300'
when b.total_people is null or b.total_people='' and b.machine_qyt>10 then '300+'
else b.total_people end as total_people,
b.machine_qyt
from
(
	select
	a.customer_id,
	a.corp_name,
	coalesce(a.t1,a.t2,null) total_people,
	count(distinct m.machine_id) machine_qyt
	from 
	(
		select
		c.customer_id,
		c.corp_name,
		c.total_people t1,
		t.total_people t2
		from rack.dwd_rack_customer c inner join  kylin.gcf_customer_total_people t on c.customer_id=t.customer_id and c.pt=20171231
	)a
	inner join 
	rack.dwd_rack_machine m on a.customer_id=m.customer_id and m.pt=20171231 and m.city_name is not null
	group by
	a.customer_id,
	a.corp_name,
	coalesce(a.t1,a.t2,null)
)b
;

---货架月度订单量

select
a.machine_id,
sum(case when c.month_short_id=6 then a.order_qyt else 0 end) order_qyt6,
count(distinct case when c.month_short_id=6 then a.user_id else null end)user_qyt6,
sum(case when c.month_short_id=6 then a.ori else 0 end) ori6,
sum(case when c.month_short_id=6 then a.act else 0 end) act6,

sum(case when c.month_short_id=7 then a.order_qyt else 0 end) order_qyt7,
count(distinct case when c.month_short_id=7 then a.user_id else null end)user_qyt7,
sum(case when c.month_short_id=7 then a.ori else 0 end) ori7,
sum(case when c.month_short_id=7 then a.act else 0 end) act7,

sum(case when c.month_short_id=8 then a.order_qyt else 0 end) order_qyt8,
count(distinct case when c.month_short_id=8 then a.user_id else null end)user_qyt8,
sum(case when c.month_short_id=8 then a.ori else 0 end) ori8,
sum(case when c.month_short_id=8 then a.act else 0 end) act8,

sum(case when c.month_short_id=9 then a.order_qyt else 0 end) order_qyt9,
count(distinct case when c.month_short_id=9 then a.user_id else null end)user_qyt9,
sum(case when c.month_short_id=9 then a.ori else 0 end) ori9,
sum(case when c.month_short_id=9 then a.act else 0 end) act9,

sum(case when c.month_short_id=10 then a.order_qyt else 0 end) order_qyt10,
count(distinct case when c.month_short_id=10 then a.user_id else null end)user_qyt10,
sum(case when c.month_short_id=10 then a.ori else 0 end) ori10,
sum(case when c.month_short_id=10 then a.act else 0 end) act10,

sum(case when c.month_short_id=11 then a.order_qyt else 0 end) order_qyt11,
count(distinct case when c.month_short_id=11 then a.user_id else null end)user_qyt11,
sum(case when c.month_short_id=11 then a.ori else 0 end) ori11,
sum(case when c.month_short_id=11 then a.act else 0 end) act11,

sum(case when c.month_short_id=12 then a.order_qyt else 0 end) order_qyt12,
count(distinct case when c.month_short_id=12 then a.user_id else null end)user_qyt12,
sum(case when c.month_short_id=12 then a.ori else 0 end) ori12,
sum(case when c.month_short_id=12 then a.act else 0 end) act12
from
(
	select
	to_char(pay_complete_time,'yyyymmdd') payday,
	machine_id,
	user_id,
	count(distinct order_id) order_qyt,
	sum(origin_amount) ori,
	sum(actual_amount) act
	from rack.dwd_rack_order
	where pt=20171231 and pay_status=64
	group by
	to_char(pay_complete_time,'yyyymmdd'),
	user_id,
	machine_id
)a inner join cdm.dim_calendar c on a.payday=c.day_id
group by
a.machine_id
;


--- 货架的类型（普通、冰柜及热柜）及采购金额、铺设时间、对应城市、公司（办公楼）类型以及覆盖的公司人数及单个货架的月度销售收入（折扣前及折扣后）、订单量
select
a.*,
b.corp_name,
b.total_people,
b.machine_qyt,

c.order_qyt6,
c.user_qyt6,
c.ori6,
c.act6,

c.order_qyt7,
c.user_qyt7,
c.ori7,
c.act7,

c.order_qyt8,
c.user_qyt8,
c.ori8,
c.act8,

c.order_qyt9,
c.user_qyt9,
c.ori9,
c.act9,

c.order_qyt10,
c.user_qyt10,
c.ori10,
c.act10,

c.order_qyt11,
c.user_qyt11,
c.ori11,
c.act11,

c.order_qyt12,
c.user_qyt12,
c.ori12,
c.act12

from
(
	select
	machine_id,
	machine_name,
	customer_id,
	machine_type_name,
	machine_sub_type_name,
	address,
	to_char(online_time,'yyyymmdd') online_date,
	case when release_status=-1  and online_time is not null then to_char(update_time,'yyyymmdd')
	else '无' end as offline_date,
	city_name
	from rack.dwd_rack_machine 
	where pt=20171231 and release_status in(2,3,-1) 
)a 
left join 
(
	select
	b.customer_id,
	b.corp_name,
	case when b.total_people is null or b.total_people='' and b.machine_qyt<4 then '30~50'
	when b.total_people is null or b.total_people='' and b.machine_qyt>3 and b.machine_qyt<11 then '100~300'
	when b.total_people is null or b.total_people='' and b.machine_qyt>10 then '300+'
	else b.total_people end as total_people,
	b.machine_qyt
	from
	(
		select
		a.customer_id,
		a.corp_name,
		coalesce(a.t1,a.t2,null) total_people,
		count(distinct m.machine_id) machine_qyt
		from 
		(
			select
			c.customer_id,
			c.corp_name,
			c.total_people t1,
			t.total_people t2
			from rack.dwd_rack_customer c inner join kylin.gcf_customer_total_people t on c.customer_id=t.customer_id and c.pt=20171231
		)a
		inner join 
		rack.dwd_rack_machine m on a.customer_id=m.customer_id and m.pt=20171231 and m.city_name is not null
		group by
		a.customer_id,
		a.corp_name,
		coalesce(a.t1,a.t2,null)
	)b
)b on a.customer_id=b.customer_id
left join 
(
	select
	a.machine_id,
	sum(case when c.month_short_id=6 then a.order_qyt else 0 end) order_qyt6,
	count(distinct case when c.month_short_id=6 then a.user_id else null end)user_qyt6,
	sum(case when c.month_short_id=6 then a.ori else 0 end) ori6,
	sum(case when c.month_short_id=6 then a.act else 0 end) act6,

	sum(case when c.month_short_id=7 then a.order_qyt else 0 end) order_qyt7,
	count(distinct case when c.month_short_id=7 then a.user_id else null end)user_qyt7,
	sum(case when c.month_short_id=7 then a.ori else 0 end) ori7,
	sum(case when c.month_short_id=7 then a.act else 0 end) act7,

	sum(case when c.month_short_id=8 then a.order_qyt else 0 end) order_qyt8,
	count(distinct case when c.month_short_id=8 then a.user_id else null end)user_qyt8,
	sum(case when c.month_short_id=8 then a.ori else 0 end) ori8,
	sum(case when c.month_short_id=8 then a.act else 0 end) act8,

	sum(case when c.month_short_id=9 then a.order_qyt else 0 end) order_qyt9,
	count(distinct case when c.month_short_id=9 then a.user_id else null end)user_qyt9,
	sum(case when c.month_short_id=9 then a.ori else 0 end) ori9,
	sum(case when c.month_short_id=9 then a.act else 0 end) act9,

	sum(case when c.month_short_id=10 then a.order_qyt else 0 end) order_qyt10,
	count(distinct case when c.month_short_id=10 then a.user_id else null end)user_qyt10,
	sum(case when c.month_short_id=10 then a.ori else 0 end) ori10,
	sum(case when c.month_short_id=10 then a.act else 0 end) act10,

	sum(case when c.month_short_id=11 then a.order_qyt else 0 end) order_qyt11,
	count(distinct case when c.month_short_id=11 then a.user_id else null end)user_qyt11,
	sum(case when c.month_short_id=11 then a.ori else 0 end) ori11,
	sum(case when c.month_short_id=11 then a.act else 0 end) act11,

	sum(case when c.month_short_id=12 then a.order_qyt else 0 end) order_qyt12,
	count(distinct case when c.month_short_id=12 then a.user_id else null end)user_qyt12,
	sum(case when c.month_short_id=12 then a.ori else 0 end) ori12,
	sum(case when c.month_short_id=12 then a.act else 0 end) act12
	from
	(
		select
		to_char(pay_complete_time,'yyyymmdd') payday,
		machine_id,
		user_id,
		count(distinct order_id) order_qyt,
		sum(origin_amount) ori,
		sum(actual_amount) act
		from rack.dwd_rack_order
		where pt=20171231 and pay_status=64
		group by
		to_char(pay_complete_time,'yyyymmdd'),
		user_id,
		machine_id
	)a inner join cdm.dim_calendar c on a.payday=c.day_id
	group by
	a.machine_id
)c on a.machine_id=c.machine_id
;


------------ 按周复购率
---人数cohort
create table gcf_user_fugou_week as
select 
week,
week_first,
week1-week_first1 cut,
count(distinct user_id) as user_num
from
(
	select 
	concat(a.year,'年',a.week_of_year,'周') week,
	concat(b.year,'年',b.week_of_year,'周') week_first,
	a.week_of_year+case when a.year=2018 then 52 else 0 end week1,
	b.week_of_year+case when b.year=2018 then 52 else 0 end week_first1,
	a.user_id,
	count(a.order_id) as order_num,
	sum(a.origin_amount) ori
	from 
	(
		select *
		from gcf_machine_trade_detail 
	) a 
	left outer join
	(
		select *
		from gcf_machine_user_day_first	
	)b 
	on a.user_id=b.user_id
	group by 
	concat(a.year,'年',a.week_of_year,'周'),
	concat(b.year,'年',b.week_of_year,'周'),
	a.week_of_year+case when a.year=2018 then 52 else 0 end ,
	b.week_of_year+case when b.year=2018 then 52 else 0 end ,
	a.user_id
) t1
group by
week,
week_first,
week1-week_first1
;

--- 复购率cohort
select
a.week_first,
a.user_num new_user,
b.cut,
b.user_num,
b.user_num/a.user_num buy_rate
from 
(
select
week_first,
user_num
from gcf_user_fugou_week 
where week=week_first
)a
inner join 
(
select
week_first,
cut,
user_num
from gcf_user_fugou_week 
where cut>0
)b on a.week_first=b.week_first
;

----- 按月coupon 1、2 补贴金额、GMV占比
select 
a.*,b.GMV,b.act
from
(
	select
	substr(o.pay_complete_time,6,2) month,
	sum(case when coupon_type=1 then discount_amount else 0 end) 1cut,
	sum(case when coupon_type=2 then discount_amount else 0 end) 2cut
	from rack.dwd_rack_order o inner join owo_ods.h_rak_order__ls_order_ordercoupon c on o.order_id=c.order_id
	and o.pt=20171231 and o.pay_status=64 and c.coupon_type in(1,2) and o.city_name is not null
	group by
	substr(o.pay_complete_time,6,2) 
)a
inner join 
(
	select 
	substr(pay_complete_time,6,2) month,
	sum(origin_amount) GMV,
	sum(actual_amount)act
	from rack.dwd_rack_order where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2)
)b on a.month=b.month
;

--- 用户CAC
select
substr(a.pay_complete_time,6,2) month,
sum(origin_amount-actual_amount) cut,
count(distinct a.user_id) new_user,
sum(origin_amount-actual_amount)/count(distinct a.user_id) CAC
from
(
select
user_id,
order_id,
origin_amount,
actual_amount,
pay_complete_time,
row_number() over (partition by user_id order by pay_complete_time asc) rn
from rack.dwd_rack_order 
where pt=20171231 and pay_status=64 
)a 
where a.rn=1 and a.origin_amount>a.actual_amount
group by
substr(a.pay_complete_time,6,2)
;


----CAC 最终版
select
substr(a.pay_complete_time,6,2) month,
sum(origin_amount-actual_amount)/count(distinct user_id) CAC
from
(
select
user_id,
order_id,
origin_amount,
actual_amount,
pay_complete_time,
row_number() over (partition by user_id order by pay_complete_time asc) rn
from rack.dwd_rack_order 
where pt=20171231 and pay_status=64 
)a 
where a.rn=1 and origin_amount>actual_amount
group by
substr(a.pay_complete_time,6,2)
;



---- SKU---品类结构
select
a.month,
a.city_name,
a.sku_id,
c.commodity_name,
div_name,
dept_name,
class_name,
subclass_name,
a.qyt,
sum(qyt)over(partition by a.month,a.city_name) city_qyt,
a.qyt/sum(qyt)over(partition by a.month,a.city_name) qyt_percent,
a.ori,
sum(a.ori)over(partition by a.month,a.city_name) city_ori,
a.ori/sum(a.ori)over(partition by a.month,a.city_name) ori_percent
from
(
	select
	substr(pay_complete_time,6,2) month,
	city_name,
	sku_id,
	sum(quantity) qyt,
	sum(commodity_origin_price*quantity)ori
	from rack.dwd_rack_order_item 
	where pt=20171231 and pay_status=64 
	group by
	substr(pay_complete_time,6,2),
	city_name,
	sku_id
)a
inner join rack.dwd_comm_commodity c on a.sku_id=c.sku_id and c.pt=20171231
group by
a.month,
a.city_name,
a.sku_id,
c.commodity_name,
div_name,
dept_name,
class_name,
subclass_name,
a.qyt,
a.ori
;

--- ROI 有优惠订单的实收/优惠金额

select 
	substr(pay_complete_time,6,2) month,
	sum(origin_amount) ori,
	sum(actual_amount)act,
	sum(origin_amount-actual_amount) cut,
	sum(actual_amount)/sum(origin_amount-actual_amount) ROI
	from rack.dwd_rack_order where pt=20171231 and pay_status=64 and origin_amount>actual_amount
	group by
	substr(pay_complete_time,6,2)
;

--- 订单数、交易额、实际货架数、动销货架数
select
b.pt daykey,
a.order_qyt,
a.ori,
a.deal_cnt,
b.online_cnt
from
(--订单数、交易额、动销货架数
	select 
	to_char(pay_complete_time,'yyyymmdd') payday,
	count(distinct order_id) order_qyt,
	sum(origin_amount) ori,
	count(distinct machine_id) deal_cnt
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by
	to_char(pay_complete_time,'yyyymmdd')
)a
inner join
(--在线货架数(日期从7月1日开始)
	select
	pt,
	count(distinct machine_id) online_cnt
	from rack.dwd_rack_machine 
	where release_status in(2,3) and machine_type in(10,20)
	group by
	pt
)b on a.payday=b.pt
;


--- 累计销售金额、累计订单量、累计销售天数、平均每日订单量、平均每日销售金额
select
c.*
from
(-- 累计金额前1000
	select
	b.month,
	b.machine_id,
	b.leiji_order,
	b.leiji_ori,
	b.leiji_days,
	b.leiji_order/b.leiji_days order_day,   -- 日均单量
	b.leiji_ori/b.leiji_days ori_day,		-- 日均交易量
	row_number() over (partition by b.month order by b.leiji_ori desc) rn
	from
	(-- 累计销售金额、累计订单量、累计销售天数
		select
		a.month,
		a.machine_id,
		sum(a.order_qyt) leiji_order,
		sum(a.ori) leiji_ori,
		count(distinct a.payday) leiji_days
		from
		(
			select
			substr(pay_complete_time,6,2) month,
			to_char(pay_complete_time,'yyyymmdd') payday,
			machine_id,
			count(distinct order_id) order_qyt,
			sum(origin_amount) ori
			from rack.dwd_rack_order
			where pt=20171231 and pay_status=64 and to_char(pay_complete_time,'yyyymmdd')>=20171101
			group by
			substr(pay_complete_time,6,2),
			to_char(pay_complete_time,'yyyymmdd'),
			machine_id
		)a
		group by
		a.month,
		a.machine_id
	)b	
)c
where c.rn<=1000
;



--- 每种人数货架每月交易量
select
a.month,
a.total_people,
count(distinct a.payday) days,
count(a.machine_id) machine_qyts, -- 动销架次
sum(order_qyt) order_qyt,
sum(ori) ori,
sum(ori)/count(a.machine_id) ori_per_machine
from
(
	select
	substr(o.pay_complete_time,6,2) month,
	to_char(o.pay_complete_time,'yyyymmdd') payday,
	o.machine_id,
	t.total_people,
	count(distinct o.order_id) order_qyt,
	sum(o.origin_amount) ori
	from rack.dwd_rack_order o 
	inner join rack.dwd_rack_machine m on o.machine_id=m.machine_id and o.pt=20171231 and m.pt=20180109 and pay_status=64
	left join kylin.gcf_customer_total_people_level t on t.customer_id=m.customer_id
	group by
	substr(o.pay_complete_time,6,2),
	to_char(o.pay_complete_time,'yyyymmdd'),
	o.machine_id,
	t.total_people
)a
group by
a.month,
a.total_people








--- 公司人数对应货架数量
select
d.total_people,
count(distinct m.machine_id) machine_qyt
from rack.dwd_rack_machine m 
inner join 
(
	select -- 公司人数分档
	c.customer_id,
	case when c.total_people<25 then '1~24'
	when c.total_people>=25 and c.total_people<50 then '25~49'
	when c.total_people>=50 and c.total_people<100 then '50~99'
	when c.total_people>=100 and c.total_people<300 then '100~299'
	when c.total_people>=300 and c.total_people<500 then '300~499'
	when c.total_people>=500 then '500+'
	else c.total_people end as total_people,
	c.machine_qyt
	from 
	(-- 每个公司具体人数
		select
		b.customer_id,
		case when b.total_people is null or b.total_people='' and b.machine_qyt<=2 then '30'
		when b.total_people is null or b.total_people='' and b.machine_qyt=3 then '50'
		when b.total_people is null or b.total_people='' and b.machine_qyt>3 and b.machine_qyt<11 then '100'
		when b.total_people is null or b.total_people='' and b.machine_qyt>10 and b.machine_qyt<17 then '300'
		when b.total_people is null or b.total_people='' and b.machine_qyt>16  then '500'
		else b.total_people end as total_people,
		b.machine_qyt
		from
		(
			select
			a.customer_id,
			a.corp_name,
			coalesce(a.t2,a.t1,null) total_people,
			count(distinct m.machine_id) machine_qyt
			from 
			(
				select
				c.customer_id,
				c.corp_name,
				c.total_people t1,
				t.total_people t2
				from rack.dwd_rack_customer c inner join  kylin.gcf_customer_total_people t on c.customer_id=t.customer_id and c.pt=20171231
			)a
			inner join 
			rack.dwd_rack_machine m on a.customer_id=m.customer_id and m.pt=20171231 and m.city_name is not null
			group by
			a.customer_id,
			a.corp_name,
			coalesce(a.t2,a.t1,null)
		)b
	)c
)d on m.customer_id=d.customer_id and m.pt=20171231 -- 货架宽表分区
and m.release_status in(2,3)
group by
d.total_people
;


create table gcf_customer_total_people_level as
select -- 公司人数分档
	c.customer_id,
	case when c.total_people<25 then '1~24'
	when c.total_people>=25 and c.total_people<50 then '25~49'
	when c.total_people>=50 and c.total_people<100 then '50~99'
	when c.total_people>=100 and c.total_people<300 then '100~299'
	when c.total_people>=300 and c.total_people<500 then '300~499'
	when c.total_people>=500 then '500+'
	else c.total_people end as total_people,
	c.machine_qyt
	from 
	(-- 每个公司具体人数
		select
		b.customer_id,
		case when b.total_people is null or b.total_people='' and b.machine_qyt<=2 then '30'
		when b.total_people is null or b.total_people='' and b.machine_qyt=3 then '50'
		when b.total_people is null or b.total_people='' and b.machine_qyt>3 and b.machine_qyt<11 then '100'
		when b.total_people is null or b.total_people='' and b.machine_qyt>10 and b.machine_qyt<17 then '300'
		when b.total_people is null or b.total_people='' and b.machine_qyt>16  then '500'
		else b.total_people end as total_people,
		b.machine_qyt
		from
		(
			select
			a.customer_id,
			a.corp_name,
			coalesce(a.t2,a.t1,null) total_people,
			count(distinct m.machine_id) machine_qyt
			from 
			(
				select
				c.customer_id,
				c.corp_name,
				c.total_people t1,
				t.total_people t2
				from rack.dwd_rack_customer c inner join  kylin.gcf_customer_total_people t on c.customer_id=t.customer_id and c.pt=20171231
			)a
			inner join 
			rack.dwd_rack_machine m on a.customer_id=m.customer_id and m.pt=20171231 and m.city_name is not null
			group by
			a.customer_id,
			a.corp_name,
			coalesce(a.t2,a.t1,null)
		)b
	)c
;


--- 累计销售金额、累计订单量、累计销售天数、平均每日订单量、平均每日销售金额(算在线天数)
select
c.month,
c.machine_id,
c.machine_name,
c.order_qyt,
c.ori,
c.online_days,
c.order_qyt/c.online_days order_day,   -- 日均单量
c.ori/c.online_days ori_day,		-- 日均交易量
c.rn
from
(
	select
	a.*,b.machine_name,b.online_days,
	row_number() over (partition by a.month order by a.ori desc) rn
	from
	( -- 累计销售金额、累计订单量、累计销售天数
		select
		a.month,
		a.machine_id,
		sum(a.order_qyt) order_qyt,
		sum(a.ori) ori
		from
		(
			select
			substr(pay_complete_time,6,2) month,
			to_char(pay_complete_time,'yyyymmdd') payday,
			machine_id,
			count(distinct order_id) order_qyt,
			sum(origin_amount) ori
			from rack.dwd_rack_order
			where pt=20171231 and pay_status=64 and to_char(pay_complete_time,'yyyymmdd')>=20171101
			group by
			substr(pay_complete_time,6,2),
			to_char(pay_complete_time,'yyyymmdd'),
			machine_id
		)a
		group by
		a.month,
		a.machine_id
	)a
	inner join 
	( -- 货架在线天数
		select
		substr(pt,5,2) month,
		machine_id,
		machine_name,
		count(distinct pt) online_days
		from rack.dwd_rack_machine
		where pt>=20171101 and pt<=20171231
		and release_status in(2,3)
		group by
		substr(pt,5,2),
		machine_id,
		machine_name
	)b on a.month=b.month and a.machine_id=b.machine_id
)c
where c.rn<=1000
;

--- 0111
--- 每月用户数量及新用户数量
select a.*,b.new_user
from
(-- 每月用户数
	select 
	substr(pay_complete_time,6,2) month,
	count(distinct user_id) user_qyt
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by 
	substr(pay_complete_time,6,2)
)a
inner join
( -- 每月新用户数
	select
	substr(rack_first_order,5,2) month,
	count(distinct user_id) new_user
	from cdm.dws_user_info_d 
	where pt=20171231
	group by
	substr(rack_first_order,5,2)
)b on a.month=b.month
;

--- 按月by sku 明细（包括GMV 和 扣除商品优惠金额等）
select 
substr(pay_complete_time,6,2) month,
sku_id,
commodity_name,
sum(quantity) qyt,
sum(commodity_origin_price*quantity) ori,
sum(commodity_sale_price*quantity) act
from rack.dwd_rack_order_item
where pt=20180108 and pay_status=64
group by
substr(pay_complete_time,6,2),
sku_id,
commodity_name
;

--- 按月新客户数、老客户数、累计购买客户数、新客客单价、老客客单价、新客单月购买频次、老客单月购买频次、新客每月消费额、老客每月消费额
select -- 累计购买用户数
a.month,
count(distinct b.user_id) user_qyt
from
(
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	1 as key
	from rack.dwd_rack_order
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)a
left join 
(
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	1 as key
	from rack.dwd_rack_order
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)b on a.key=b.key 
where a.month>=b.month 
group by
a.month
;

--- 累计用户数
select
	
	count(distinct user_id)
	from rack.dwd_rack_order
	where pt=20170630 and pay_status=64
;

--- 新客单月购买频次
select
a.first_month,
avg(b.order_qyt) avg_order
from 
( -- 用户首月
	select
	substr(rack_first_order,5,2) first_month,
	user_id
	from cdm.dws_user_info_d 
	where pt=20171231
)a
inner join 
( -- 用户每月订单量
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	count(distinct order_id) order_qyt
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)b on a.user_id=b.user_id
where a.first_month=b.month
group by
a.first_month
;

-- 老用户月购买频次
select
b.month,
avg(b.order_qyt) avg_order
from 
( -- 用户首月
	select
	substr(rack_first_order,5,2) first_month,
	user_id
	from cdm.dws_user_info_d 
	where pt=20171231
)a
inner join 
( -- 用户每月订单量
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	count(distinct order_id) order_qyt
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)b on a.user_id=b.user_id
where a.first_month<b.month
group by
b.month
;

--- 每月累计用户数
select  /*+ MapJoin(a) */
  a.month_id,
  count(distinct b.user_id) user_qty
from 
(
	select month_id from cdm.dim_calendar where year_id=2017 group by month_id
) a 
join (
	select 
		to_char(pay_complete_time,'yyyymm') month_id,
		user_id
	from 
		rack.dwd_rack_order_item
	where 
		pt=20171231 
	and 
		pay_status=64
	group by 
		to_char(pay_complete_time,'yyyymm'),
		user_id
) b 
on 
	a.month_id >= b.month_id
group by 
	 a.month_id
;

-- 新客每月消费额
select
b.month,
avg(b.ori) avg_order
from 
( -- 用户首月
	select
	substr(rack_first_order,5,2) first_month,
	user_id
	from cdm.dws_user_info_d 
	where pt=20171231
)a
inner join 
( -- 用户每月订单量
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	sum(origin_amount) ori
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)b on a.user_id=b.user_id
where a.first_month=b.month
group by
b.month
;


-- 新客每月订单量
select
b.month,
sum(b.order_qyt) order_qyt
from 
( -- 用户首月
	select
	substr(rack_first_order,5,2) first_month,
	user_id
	from cdm.dws_user_info_d 
	where pt=20171231
)a
inner join 
( -- 用户每月订单量
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	count(distinct order_id) order_qyt
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)b on a.user_id=b.user_id
where a.first_month=b.month
group by
b.month
;




--- 12个点位订单明细（包括GMV、sku等信息）
select
i.machine_id,
a.machine_name,
a.total_people,
i.order_id,
o.origin_amount,
i.pay_complete_time,
i.sku_id,
i.commodity_name,
i.commodity_origin_price,
i.commodity_sale_price,
i.quantity
from rack.dwd_rack_order_item i
inner join rack.dwd_rack_order o on i.order_id=o.order_id and o.pt=20171231 and i.pt=20171231 and o.pay_status=64
inner join 
(
select
machine_id,machine_name,t.total_people
from rack.dwd_rack_machine m
inner join kylin.gcf_customer_total_people_mingxi t on m.customer_id=t.customer_id and pt=20171231
where machine_name in
('强生-冰柜',
'厚冠信息-冰柜',
'小站教育',
'理优教育',
'管易软件-冰柜',
'恒昌解放碑冰柜1',
'上海新康1',
'上海新康-冰柜1',
'恒昌冰柜1',
'锐战-冰柜',
'上海墨鵾数码科技2',
'金裕道-货架1')
)a on a.machine_id=i.machine_id 
;



-- 6月优惠订单
select
o.order_id,
i.*
from rack.dwd_rack_order o inner join rack.dwd_rack_order_item i on o.order_id=i.order_id
and o.pt=20171231 and i.pt=20171231 and o.pay_status=64
where substr(o.pay_complete_time,6,2)='06'
and o.origin_amount>o.actual_amount
;


--- 老用户客单价
select
b.month,
avg(b.avg_ori) avg_ori
from 
( -- 用户首月
	select
	substr(rack_first_order,5,2) first_month,
	user_id
	from cdm.dws_user_info_d 
	where pt=20171231
)a
inner join 
( -- 用户每月客单价
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	avg(origin_amount) avg_ori
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)b on a.user_id=b.user_id
where a.first_month<b.month
group by
b.month
;

--- 新用户客单价(最后使用)
select
b.month,
sum(b.ori)/sum(b.order_qyt)
from 
( -- 用户首月
	select
	substr(rack_first_order,5,2) first_month,
	user_id
	from cdm.dws_user_info_d 
	where pt=20171231
)a
inner join 
( -- 用户每月客单价
	select
	substr(pay_complete_time,6,2) month,
	user_id,
	sum(origin_amount) ori,
	count(distinct order_id) order_qyt
	from rack.dwd_rack_order 
	where pt=20171231 and pay_status=64
	group by
	substr(pay_complete_time,6,2),
	user_id
)b on a.user_id=b.user_id
where a.first_month=b.month
group by
b.month
;


--- 每月工作日订单数、GMV、折扣额、工作日用户数
select
a.month,
count(distinct a.user_id) user_qyt,
sum(a.order_qyt) order_qyt,
sum(a.ori) ori,
sum(a.act) act
from
(
	select
	substr(pay_complete_time,6,2) month,
	to_char(pay_complete_time,'yyyymmdd') payday,
	user_id,
	count(distinct order_id) order_qyt,
	sum(origin_amount) ori,
	sum(actual_amount) act
	from rack.dwd_rack_order 
	where pay_status=64 and pt=20171231
	group by 
	substr(pay_complete_time,6,2),
	to_char(pay_complete_time,'yyyymmdd'),
	user_id
)a
inner join 
cdm.dim_calendar c on a.payday=c.day_id and c.day_type='工作日'
group by
a.month
;


--- 存货周转天数（满库存pcs/每天售卖pcs）
select -- 满库存pcs
avg(a.max_inventory) max_inventory
from 
(
	select 
	machine_id,
	count(distinct sku_id) sku_qyt,
	sum(max_inventory) max_inventory
	from owo_ods.kylin__machine_commodity_info_his  
	where is_valid=1 and day_id=20170630
	group by machine_id
)a
;

select -- 单货架每天售卖pcs
avg(b.qyt)
from 
(
	select
	a.machine_id,
	avg(a.qyt) qyt
	from
	(
		select 
		machine_id,
		to_char(pay_complete_time,'yyyymmdd') payday,
		sum(quantity) qyt
		from rack.dwd_rack_order_item 
		where pay_status=64 and pt=20171231
		and pay_complete_time>'2017-12-01 00:00:00'
		group by
		machine_id,
		to_char(pay_complete_time,'yyyymmdd')
	)a
	inner join 
	cdm.dim_calendar c on a.payday=c.day_id and c.day_type='工作日'
	group by a.machine_id
)b
;

-- sku 添加补充涉及货架数、涉及用户数、涉及订单量
select 
substr(pay_complete_time,6,2) month,
sku_id,
commodity_name,
sum(quantity) qyt,
sum(commodity_origin_price*quantity) ori,
sum(commodity_sale_price*quantity) act,
count(distinct machine_id) machine_qyt,
count(distinct user_id) user_qyt,
count(distinct order_id) order_qyt
from rack.dwd_rack_order_item
where pt=20171231 and pay_status=64
group by
substr(pay_complete_time,6,2),
sku_id,
commodity_name
;


---工作日平均售卖货架数、订单量
select
a.month,
avg(a.machine_qyt)
from
(
	select
	substr(pay_complete_time,6,2) month,
	to_char(pay_complete_time,'yyyymmdd') payday,
	count(distinct order_id) order_qyt,
	count(distinct machine_id) machine_qyt,
	count(distinct order_id)/count(distinct machine_id) order_mac
	from rack.dwd_rack_order 
	where pay_status=64 and pt=20171231
	group by 
	to_char(pay_complete_time,'yyyymmdd')
)a
inner join 
cdm.dim_calendar c on a.payday=c.day_id and c.day_type='工作日'
group by
a.month
;


-- 每个货架11月12月的订单量, 交易额, 实付额, 成本额, 用户数, 当月新增用户数
select
substr(c.payday,5,2) month,
c.machine_id,
m.machine_name,
sum(c.order_qyt) order_qyt,
sum(c.ori) ori,
sum(c.act) act,
sum(p.移动进价)移动进价,
sum(c.user_qyt) user_qyt,
sum(c.new_user) new_user
from
(
	select
	a.payday,
	a.machine_id,
	sum(a.order_qyt) order_qyt,
	sum(a.ori) ori,
	sum(a.act) act,
	count(distinct a.user_id) user_qyt,
	count(distinct case when a.payday=b.rack_first_order then a.user_id else null end) new_user
	from
	(
		select
		to_char(pay_complete_time,'yyyymmdd') payday,
		machine_id,
		user_id,
		count(distinct order_id) order_qyt,
		sum(origin_amount) ori,
		sum(actual_amount) act
		from rack.dwd_rack_order 
		where pt=20171231 and pay_status=64
		and to_char(pay_complete_time,'yyyymmdd')>=20171101
		group by
		to_char(pay_complete_time,'yyyymmdd'),
		machine_id,
		user_id
	)a
	left join 
	( -- 用户首月
		select
		rack_first_order,
		user_id
		from cdm.dws_user_info_d 
		where pt=20171231
	)b on a.user_id=b.user_id
	group by
	a.payday,
	a.machine_id
)c
left join 
(select pt,货架id,订单数,原售价,实售价,移动进价 from kylin.shawn_profit_detail group by pt,货架id,订单数,原售价,实售价,移动进价) p on c.payday=p.pt and c.machine_id=p.货架id
left join rack.dwd_rack_machine m on c.machine_id=m.machine_id and m.pt=20171231
group by
substr(c.payday,5,2),
c.machine_id,
m.machine_name
;

--第二，8万点位按人数的分布比例；第三，按公司数，订单和GMV数据

-- 货架数
select
b.total_people,
count(distinct b.machine_id) machine_qyt
from
(
	select
	m.machine_id,
	a.customer_id,
	a.total_people
	from
	(
		select 
		customer_id,
		case when c.total_people<30 then '30-'
		when c.total_people>=30 and c.total_people<50 then '30~49'
		when c.total_people>=50 and c.total_people<100 then '50~99'
		when c.total_people>=100 and c.total_people<300 then '100~299'
		when c.total_people>=300 then '300+'
		else c.total_people end as total_people
		from kylin.gcf_customer_total_people_mingxi c
	)a
	inner join rack.dwd_rack_machine m on m.customer_id=a.customer_id and m.pt=20171231 and m.release_status in(2,3)
)b
group by
b.total_people
;

-- 7月数据（货架未关联公司）


select
b.total_people,
count(distinct b.machine_id) machine_qyt
from
(
	select
	m.machine_id,
	a.customer_id,
	a.total_people
	from
	(
		select 
		customer_id,
		case when c.total_people<30 then '30-'
		when c.total_people>=30 and c.total_people<50 then '30~49'
		when c.total_people>=50 and c.total_people<100 then '50~99'
		when c.total_people>=100 and c.total_people<300 then '100~299'
		when c.total_people>=300 then '300+'
		else c.total_people end as total_people
		from kylin.gcf_customer_total_people_mingxi c
	)a
	inner join 
	(
		select a.machine_id,r.customer_id
		from 
		(select machine_id from rack.dwd_rack_machine where pt=20170731 and release_status in(2,3))a 
		inner join rack.dwd_rack_machine r on a.machine_id=r.machine_id and r.pt=20171231
	)m on a.customer_id=m.customer_id
)b
group by
b.total_people


-- 订单、GMV
select
c.month,
c.total_people,
sum(c.order_qyt) order_qyt,
sum(c.GMV) GMV
from
(
	select
	substr(o.pay_complete_time,6,2) month,
	o.machine_id,
	b.total_people,
	count(distinct o.order_id) order_qyt,
	sum(o.origin_amount) GMV
	from 
	rack.dwd_rack_order o
	left join 
	(
		select
		m.machine_id,
		a.customer_id,
		a.total_people
		from
		(
			select 
			customer_id,
			case when c.total_people<30 then '30-'
			when c.total_people>=30 and c.total_people<50 then '30~49'
			when c.total_people>=50 and c.total_people<100 then '50~99'
			when c.total_people>=100 and c.total_people<300 then '100~299'
			when c.total_people>=300 then '300+'
			else c.total_people end as total_people
			from kylin.gcf_customer_total_people_mingxi c
		)a
		inner join rack.dwd_rack_machine m on m.customer_id=a.customer_id and m.pt=20171231 
	)b on o.machine_id=b.machine_id and o.pt=20171231 and o.pay_status=64 
	where b.machine_id is not null
	group by
	substr(o.pay_complete_time,6,2),
	o.machine_id,
	b.total_people
)c
group by
c.month,
c.total_people
;

-- 消费人数
select
c.month,
c.total_people,
sum(c.user_qyt) user_qyt
from
(
	select
	substr(o.pay_complete_time,6,2) month,
	b.customer_id,
	b.total_people,
	count(distinct o.user_id) user_qyt
	from 
	rack.dwd_rack_order o
	left join 
	(
		select
		m.machine_id,
		a.customer_id,
		a.total_people
		from
		(
			select 
			customer_id,
			case when c.total_people<30 then '30-'
			when c.total_people>=30 and c.total_people<50 then '30~49'
			when c.total_people>=50 and c.total_people<100 then '50~99'
			when c.total_people>=100 and c.total_people<300 then '100~299'
			when c.total_people>=300 then '300+'
			else c.total_people end as total_people
			from kylin.gcf_customer_total_people_mingxi c
		)a
		inner join rack.dwd_rack_machine m on m.customer_id=a.customer_id and m.pt=20171231 
	)b on o.machine_id=b.machine_id and o.pt=20171231 and o.pay_status=64 
	where b.machine_id is not null
	group by
	substr(o.pay_complete_time,6,2),
	b.customer_id,
	b.total_people
)c
group by
c.month,
c.total_people
;

-- 12月底累计人数
select
b.total_people,
count(distinct o.user_id) user_qyt
from 
rack.dwd_rack_order o
left join 
(
	select
	m.machine_id,
	a.customer_id,
	a.total_people
	from
	(
		select 
		customer_id,
		case when c.total_people<30 then '30-'
		when c.total_people>=30 and c.total_people<50 then '30~49'
		when c.total_people>=50 and c.total_people<100 then '50~99'
		when c.total_people>=100 and c.total_people<300 then '100~299'
		when c.total_people>=300 then '300+'
		else c.total_people end as total_people
		from kylin.gcf_customer_total_people_mingxi c
	)a
	inner join rack.dwd_rack_machine m on m.customer_id=a.customer_id and m.pt=20171231 
)b on o.machine_id=b.machine_id and o.pt=20171231 and o.pay_status=64 
where b.machine_id is not null
group by
b.total_people
;


--- 商品一级品类11、12月销量
select
a.month,
c.div_name,
sum(a.order_qyt) order_qyt,
sum(a.qyt) qyt
from
(
select
substr(pay_complete_time,6,2) month,
sku_id,
count(distinct order_id) order_qyt,
sum(quantity) qyt
from rack.dwd_rack_order_item where pt=20171231 and pay_status=64 and substr(pay_complete_time,6,2) in(11,12)
group by 
substr(pay_complete_time,6,2),
sku_id
)a 
inner join rack.dwd_comm_commodity c on a.sku_id=c.sku_id and c.pt=20171231
group by 
a.month,
c.div_name
;

 -- 1130 1228 货架分级货架数
select
pt,
machine_grading,
count(distinct machine_id) machine_qyt
from rack.dws_machine_grading where pt in(20171130,20171228)
group by
pt,
machine_grading
;


-- 0117  1月数据
select 
sum(origin_amount) ori,
count(distinct order_id) order_qyt,

from rack.dwd_rack_order 
where pt=20180116 and pay_status=64 and to_char(pay_complete_time,'yyyymmdd')>=20180101
;