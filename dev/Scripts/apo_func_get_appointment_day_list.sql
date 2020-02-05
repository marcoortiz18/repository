CREATE OR REPLACE FUNCTION appointment.apo_func_get_appointment_day_list(p_bof_id numeric, p_date_from date)
 RETURNS TABLE(date_generate date, day_number numeric, error_code text, user_message text, api_message text, basic_requeriments text)
 LANGUAGE plpgsql
AS $function$
 
declare 
/*PARA ERRORES*/
v_state text;
v_msg text;
v_detail text;
v_error_code text;
v_api_message text;
v_user_message text;
v_basic_requeriments text;
BEGIN

return query  
	 select y.date_gen::date as fecha, 
        	y.day_number as dia,
			null::text,
			null::text,  
			null::text,
			null::text
	   from (select x.date_gen::date, 
					extract(ISODOW from date_gen::date)::numeric as day_number,
					count_per_day,
					count(apm_id) as cant
	   		   from (select date_gen
			   		   from generate_series(p_date_from::date, p_date_from+10, '1 day'::interval) date_gen
			 		) x
	     inner join configuration.cnf_tbl_branch_office_process
		         on bop_prs_id = 2
		        and bop_bof_id = p_bof_id
		        and bop_day_number = extract(ISODOW from x.date_gen::date)::numeric
	     inner join configuration.cnf_func_get_count_per_day_special_date(bop_day, bop_bof_id, bop_prs_id, bop_count_per_day, x.date_gen::date) as count_per_day
		         on 1 = 1
	      left join appointment.apo_tbl_appointment
		         on apm_date::date = x.date_gen and apm_bof_id = p_bof_id
	          --where extract(ISODOW from x.date_gen::date) <> 7
	       group by x.date_gen::date, count_per_day 
		   order by 1 asc
	          limit 7) y
	where cant < count_per_day;

EXCEPTION 
WHEN others THEN
GET STACKED DIAGNOSTICS
	v_state = RETURNED_SQLSTATE,
	v_msg = MESSAGE_TEXT,
	v_detail = PG_EXCEPTION_DETAIL;
	raise notice 'error';
	raise notice '%',v_state;
	raise notice '%',v_msg;
	raise notice '%',v_detail;
	
	PERFORM 
			configuration.cnf_func_post_error_function_log('SELECT * FROM appointment.apo_func_get_appointment_day_list('||case when p_bof_id::text is null then 'null' else p_bof_id::text end||','||case when p_date_from::text is null then 'null' else p_date_from::text end||')',msg_code, msg_user_message, msg_api_message, msg_basic_requirement, v_detail, v_state, v_msg)
		from
			configuration.cnf_tbl_message
		where
			msg_code = case when v_error_code is null then 'M0000' else v_error_code end;
			
	return query
		select null::date,
		       null::numeric,
			   msg_code,
			   msg_user_message,
			   msg_api_message,
			   msg_basic_requirement
		  from configuration.cnf_tbl_message
		 where msg_code = 'M0000';
	
END; 

$function$
;
