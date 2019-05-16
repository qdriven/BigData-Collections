create schema if not exists quality collate utf8mb4_0900_ai_ci;

create table if not exists data_connector
(
	id int auto_increment
		primary key,
	name varchar(20) not null comment 'data source uages',
	type varchar(10) null comment 'data source type, enum in DataType',
	config json not null comment 'datasource configuration in json format',
	created_date timestamp null comment 'create date',
	modified_date timestamp null comment 'timestamp'
)
comment 'data connector configuration';

create table if not exists data_record_statistics
(
	id bigint auto_increment
		primary key,
	db_name varchar(30) null comment 'database name',
	rows_count bigint null,
	count_date date null,
	created_date date null,
	modified_date date null,
	table_name varchar(30) null
)
comment '每日数据汇总';

create table if not exists data_validation_statistics
(
	id bigint auto_increment
		primary key,
	db_name varchar(30) null,
	table_name varchar(30) null,
	potential_error_count bigint null,
	count_date date null,
	run_sql varchar(3000) null,
	create_date date null,
	modified_date date null
)
comment '验证结果';

