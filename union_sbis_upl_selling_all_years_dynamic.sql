create or replace procedure sbis_append_table_if_exists(
    p_target_table text,
    p_source_table text
)
language plpgsql
as $$
begin
    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = p_source_table
    ) then
        execute format(
            'insert into %I select * from %I',
            p_target_table,
            p_source_table
        );
    else
        raise notice 'False %', p_source_table;
    end if;
end;
$$;


create or replace procedure sbis_prepare_week_table(
    p_table_name text
)
language plpgsql
as $$
declare
    v_column_name text;
begin
    if not exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = p_table_name
    ) then
        return;
    end if;

    foreach v_column_name in array array[
        'inside_doc_item_full_doc_price',
        'inside_doc_item_unit',
        'inside_license_type',
        'inside_doc_item_article',
        'inside_doc_item_full_item_price',
        'inside_doc_item_sn',
        'inside_warehouse'
    ]
    loop
        if exists (
            select 1
            from information_schema.columns
            where table_schema = 'public'
              and table_name = p_table_name
              and column_name = v_column_name
        ) then
            execute format(
                'alter table %I alter column %I type text',
                p_table_name,
                v_column_name
            );
        end if;
    end loop;
end;
$$;


create or replace procedure sbis_append_week_month(
    p_target_table text,
    p_month_code text
)
language plpgsql
as $$
declare
    v_week_table text;
    v_t_week_fallback text;
begin
    v_week_table := format('sbis_week_%s_end_first_week_uploading', p_month_code);
    call sbis_prepare_week_table(v_week_table);
    call sbis_append_table_if_exists(p_target_table, v_week_table);

    v_week_table := format('sbis_week_%s_end_s_week_uploading', p_month_code);
    call sbis_prepare_week_table(v_week_table);
    call sbis_append_table_if_exists(p_target_table, v_week_table);

    v_week_table := format('sbis_week_%s_end_t_week_uploading', p_month_code);
    v_t_week_fallback := format('sbis_week_2%s_end_t_week_uploading', p_month_code);

    if exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = v_week_table
    ) then
        call sbis_prepare_week_table(v_week_table);
        call sbis_append_table_if_exists(p_target_table, v_week_table);
    elsif exists (
        select 1
        from information_schema.tables
        where table_schema = 'public'
          and table_name = v_t_week_fallback
    ) then
        call sbis_prepare_week_table(v_t_week_fallback);
        call sbis_append_table_if_exists(p_target_table, v_t_week_fallback);
    else
        raise notice 'False %, %', v_week_table, v_t_week_fallback;
    end if;

    v_week_table := format('sbis_week_%s_end_f_week_uploading', p_month_code);
    call sbis_prepare_week_table(v_week_table);
    call sbis_append_table_if_exists(p_target_table, v_week_table);

    v_week_table := format('sbis_week_%s_end_r_week_uploading', p_month_code);
    call sbis_prepare_week_table(v_week_table);
    call sbis_append_table_if_exists(p_target_table, v_week_table);
end;
$$;


create or replace procedure sbis_build_year_monthly(
    p_year integer
)
language plpgsql
as $$
declare
    v_target_table text;
    v_source_table text;
    v_month integer;
begin
    v_target_table := format('sbis_upl_%s', p_year);

    execute format('drop table if exists %I', v_target_table);
    execute format(
        'create table %I as (select * from sbis_20230101_uploading limit 0)',
        v_target_table
    );

    for v_month in 1..12
    loop
        v_source_table := format(
            'sbis_%s%s01_uploading',
            p_year,
            lpad(v_month::text, 2, '0')
        );
        call sbis_append_table_if_exists(v_target_table, v_source_table);
    end loop;
end;
$$;


create or replace procedure sbis_build_year_2024()
language plpgsql
as $$
declare
    v_source_table text;
    v_month integer;
begin
    drop table if exists sbis_upl_2024_monthly;
    create table sbis_upl_2024_monthly as (select * from sbis_20240101_uploading limit 0);

    for v_month in 1..9
    loop
        v_source_table := format(
            'sbis_2024%s01_uploading',
            lpad(v_month::text, 2, '0')
        );
        call sbis_append_table_if_exists('sbis_upl_2024_monthly', v_source_table);
    end loop;

    drop table if exists sbis_upl_2024_weekly;
    create table sbis_upl_2024_weekly as (select * from sbis_20240101_uploading limit 0);

    call sbis_append_week_month('sbis_upl_2024_weekly', '202410');
    call sbis_append_week_month('sbis_upl_2024_weekly', '202411');
    call sbis_append_week_month('sbis_upl_2024_weekly', '202412');

    drop table if exists sbis_upl_2024;
    create table sbis_upl_2024 as
    select * from sbis_upl_2024_monthly
    union all
    select * from sbis_upl_2024_weekly;
end;
$$;


create or replace procedure sbis_build_year_weekly(
    p_year integer
)
language plpgsql
as $$
declare
    v_target_table text;
    v_month integer;
    v_month_code text;
begin
    v_target_table := format('sbis_upl_%s', p_year);

    execute format('drop table if exists %I', v_target_table);
    execute format(
        'create table %I as (select * from sbis_20240101_uploading limit 0)',
        v_target_table
    );

    for v_month in 1..12
    loop
        v_month_code := format(
            '%s%s',
            p_year,
            lpad(v_month::text, 2, '0')
        );
        call sbis_append_week_month(v_target_table, v_month_code);
    end loop;
end;
$$;


create or replace procedure union_sbis_upl_selling_all_years(
    p_last_year integer default extract(year from current_date)::integer
)
language plpgsql
as $$
declare
    v_year integer;
begin
    call sbis_build_year_monthly(2020);
    call sbis_build_year_monthly(2021);
    call sbis_build_year_monthly(2022);
    call sbis_build_year_monthly(2023);

    call sbis_build_year_2024();

    if p_last_year >= 2025 then
        for v_year in 2025..p_last_year
        loop
            call sbis_build_year_weekly(v_year);
        end loop;
    end if;

    drop table if exists sbis_upl_selling.sbis_upl_selling;

    create table sbis_upl_selling.sbis_upl_selling as
    select * from sbis_upl_2020
    union all
    select * from sbis_upl_2021
    union all
    select * from sbis_upl_2022
    union all
    select * from sbis_upl_2023
    union all
    select * from sbis_upl_2024;

    if p_last_year >= 2025 then
        for v_year in 2025..p_last_year
        loop
            execute format(
                'insert into sbis_upl_selling.sbis_upl_selling select * from %I',
                format('sbis_upl_%s', v_year)
            );
        end loop;
    end if;

    alter table sbis_upl_selling.sbis_upl_selling
    drop column if exists index;

    alter table sbis_upl_selling.sbis_upl_selling
    add column index serial;
end;
$$;


call union_sbis_upl_selling_all_years();


drop table sbis_upl_selling.sbis_upl_selling;


