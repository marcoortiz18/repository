CREATE OR REPLACE FUNCTION appointment.apo_func_get_appointment(p_start_date character varying, p_end_date character varying, p_location character varying, p_email character varying, p_appointment_id character varying, p_phone character varying, p_name character varying)
 RETURNS TABLE(user_name character varying, user_last_name character varying, user_email character varying, user_phone character varying, appointment_id numeric, stock_id character varying, make character varying, model character varying, version character varying, year character varying, user_id character varying, price_appointment_date character varying, location_id character varying, center_location character varying, vin character varying, km character varying, transmission character varying, image_catalogue character varying, url_product character varying, status character varying, appointment_date character varying, hour_block_id character varying, checkpoint_id numeric, appointment_creation_date text, car_location_url text, warehouse_location text, id_car_location numeric, error_code text, user_message text, api_message text, basic_requeriments text)
 LANGUAGE plpgsql
AS $function$

DECLARE
v_image_url text;
v_kavak_url text;
v_record numeric; 
/*PARA ERRORES*/
v_error_code text;  
v_api_message text;
v_user_message text;
v_basic_requeriments text; 
v_state text;
v_msg text;
v_detail text;

BEGIN
select 
	par_value
into
	v_image_url
from
	configuration.cnf_tbl_parameter   
where
	par_name = 'image_url';

select 
	par_value
into
	v_kavak_url
from
	configuration.cnf_tbl_parameter
where
	par_name = 'url_kavak_website';
select
	count(*)
into v_record
from
	appointment.apo_tbl_appointment
left join
	transaction.trn_tbl_buy_checkpoint
	on bcp_apm_id = apm_id 
left join
	product.pro_tbl_catalogue_view
	on mtc_id = apm_ctl_id
left join
	product.pro_tbl_catalogue_image
	on cti_ctl_id = apm_ctl_id and cti_order = 1
left join 
	"user".usr_tbl_user
	on apm_use_id = use_id
left join 
	"user".usr_tbl_user_phone
	on use_id = uph_use_id and uph_is_main = 1 and uph_is_active = 1
left join
	(select bom_url, bom_bof_id from geography.geo_tbl_branch_office_media where bom_type = 'short map' and bom_is_active = 1 limit 1) x
	on x.bom_bof_id = apm_bof_id
where
	case when p_appointment_id is null then(
	apm_date between p_start_date::date and p_end_date::date
	or 
	apm_bof_id = p_location::numeric
	or
	use_email = p_email
	or 
	uph_phone like '%'||p_phone||'%'
	or 
	use_name || ' ' || use_last_name like '%'||p_name||'%')
	else
	bcp_id = p_appointment_id::numeric
	end
	and apm_is_active = 1;
if v_record > 0 then
return query
select
	use_name::character varying,
	use_last_name::character varying,
	use_email::character varying,
	uph_phone::character varying,
	apm_id,
	apm_ctl_id::character varying,
	mtc_car_make::character varying,
	mtc_car_model::character varying,
	mtc_car_trim::character varying,
	mtc_car_year::character varying,
	use_id::character varying,
	bcp_price::character varying,
	apm_bof_id::character varying,
	bof_short_name::character varying,
	mtc_vin::character varying,
	mtc_km::character varying,
	mtc_transmission::character varying,
	(v_image_url||cti_url)::character varying,
	(v_kavak_url||mtc_car_url)::character varying,
	mtc_status::character varying,
	apm_date::character varying,
	apm_sch_id::character varying,
	bcp_id,
	apm_creation_date::text,
	bom_url::text,
	mtc_location_filter::text,
	mtc_location_id,
	null::text,
	null::text,
	null::text,
	null::text
from
	appointment.apo_tbl_appointment
inner join
	transaction.trn_tbl_buy_checkpoint
	on bcp_apm_id = apm_id 
left join
	product.pro_tbl_catalogue_view
	on mtc_id = apm_ctl_id
left join
	product.pro_tbl_catalogue_image
	on cti_ctl_id = apm_ctl_id and cti_order = 1
left join 
	"user".usr_tbl_user
	on apm_use_id = use_id
left join 
	"user".usr_tbl_user_phone
	on use_id = uph_use_id and uph_is_main = 1 and uph_is_active = 1
left join
	(select distinct(bom_bof_id) as sd, bom_url, bom_bof_id from geography.geo_tbl_branch_office_media where bom_type = 'short map' and bom_is_active = 1) x
	on x.bom_bof_id = apm_bof_id
left join
	geography.geo_tbl_branch_office
	on bof_id = apm_bof_id
where
	case when p_appointment_id is null then(
	apm_date between p_start_date::date and p_end_date::date
	or 
	apm_bof_id = p_location::numeric
	or
	use_email = p_email
	or 
	uph_phone like '%'||p_phone||'%'
	or 
	use_name || ' ' || use_last_name like '%'||p_name||'%')
	else
	bcp_id = p_appointment_id::numeric
	end
	and apm_is_active = 1;

else 
RETURN QUERY 
	select
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::numeric,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::character varying,
		null::numeric,
		null::text,
		null::text,
		null::text,
		null::numeric,
		null::text,
		null::text,
		null::text,
		null::text
	from configuration.cnf_tbl_message
	where msg_code = 'M0000'; 
end if;

--LC 12/03/2019 se agrega bloque de excepcion
EXCEPTION 
    WHEN others THEN
        GET STACKED DIAGNOSTICS
            v_state  = RETURNED_SQLSTATE,
            v_msg    = MESSAGE_TEXT,
            v_detail = PG_EXCEPTION_DETAIL;

            raise info '-- %',v_state;
            raise info '-- %',v_msg;
            raise info '-- %',v_detail;
       --LC 13/06/2019 se ajusta el llamado a la función para eliminar campos que no son parámetros de esta función      
		PERFORM 
                        configuration.cnf_func_post_error_function_log('SELECT * FROM appointment.apo_func_get_appointment('||case when p_start_date::text is null then 'null' else p_start_date::text end||','||case when p_end_date::text is null then 'null' else p_end_date::text end||','||case when p_location::text is null then 'null' else p_location::text end||','||case when p_email::text is null then 'null' else p_email::text end||','||case when p_appointment_id::text is null then 'null' else p_appointment_id::text end||','||case when p_phone::text is null then 'null' else p_phone::text end||','||case when p_name::text is null then 'null' else p_name::text end||')',msg_code, msg_user_message, msg_api_message, msg_basic_requirement, v_detail, v_state, v_msg)
                from
                        configuration.cnf_tbl_message
                where
                        msg_code = case when v_error_code is null then 'M0000' else v_error_code end;
            RETURN QUERY 
                    select
                        null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::numeric,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::character varying,
						null::numeric,
						null::text,
						null::text,
						null::text,
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
