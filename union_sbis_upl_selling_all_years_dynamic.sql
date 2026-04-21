create or replace procedure sbis_append_table_if_exists(
    p_target_table text,
    p_source_table text
)
language plpgsql
as $$
declare
    v_target_regclass regclass;
    v_source_regclass regclass;
    v_target_column_list text;
    v_source_select_list text;
    v_matching_column_count integer;
begin
    v_target_regclass := to_regclass(p_target_table);
    v_source_regclass := to_regclass(p_source_table);

    if v_target_regclass is null then
        raise exception 'Target table does not exist: %', p_target_table;
    end if;

    if v_source_regclass is not null then
        select
            string_agg(quote_ident(tgt.attname), ', ' order by tgt.attnum),
            string_agg(
                case
                    when src.attname is not null then format(
                        'coalesce(%1$I::text, %2$L) as %1$I',
                        tgt.attname,
                        ''
                    )
                    else format(
                        '%L as %I',
                        '',
                        tgt.attname
                    )
                end,
                ', '
                order by tgt.attnum
            ),
            count(src.attname)
        into v_target_column_list, v_source_select_list, v_matching_column_count
        from pg_attribute tgt
        left join pg_attribute src
          on src.attrelid = v_source_regclass
         and src.attname = tgt.attname
         and src.attnum > 0
         and not src.attisdropped
        where tgt.attrelid = v_target_regclass
          and tgt.attnum > 0
          and not tgt.attisdropped;

        if v_target_column_list is null or v_matching_column_count = 0 then
            raise notice 'No matching columns between % and %', p_target_table, p_source_table;
            return;
        end if;

        execute format(
            'insert into %s (%s) select %s from %s',
            v_target_regclass,
            v_target_column_list,
            v_source_select_list,
            v_source_regclass
        );
    else
        raise notice 'False %', p_source_table;
    end if;
end;
$$;


create or replace procedure sbis_create_union_table(
    p_target_table text,
    p_source_tables text[]
)
language plpgsql
as $$
declare
    v_target_parts text[];
    v_target_schema text;
    v_target_name text;
    v_column_defs text;
begin
    v_target_parts := pg_catalog.parse_ident(p_target_table);

    if array_length(v_target_parts, 1) = 2 then
        v_target_schema := v_target_parts[1];
        v_target_name := v_target_parts[2];
    elsif array_length(v_target_parts, 1) = 1 then
        v_target_schema := current_schema;
        v_target_name := v_target_parts[1];
    else
        raise exception 'Invalid target table name: %', p_target_table;
    end if;

    with source_tables as (
        select
            tbl_name,
            ord
        from unnest(p_source_tables) with ordinality as src(tbl_name, ord)
        where to_regclass(tbl_name) is not null
    ),
    source_columns as (
        select
            src.ord,
            a.attnum,
            a.attname
        from source_tables src
        join pg_attribute a
          on a.attrelid = to_regclass(src.tbl_name)
        where a.attnum > 0
          and not a.attisdropped
    ),
    first_seen as (
        select distinct on (attname)
            attname,
            ord,
            attnum
        from source_columns
        order by attname, ord, attnum
    )
    select string_agg(
        format('%I text', first_seen.attname),
        ', '
        order by first_seen.ord, first_seen.attnum
    )
    into v_column_defs
    from first_seen
    ;

    if v_column_defs is null then
        raise exception 'No source tables found to build %', p_target_table;
    end if;

    execute format(
        'drop table if exists %I.%I',
        v_target_schema,
        v_target_name
    );

    execute format(
        'create table %I.%I (%s)',
        v_target_schema,
        v_target_name,
        v_column_defs
    );
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
    v_source_tables text[] := '{}';
begin
    v_target_table := format('sbis_upl_%s', p_year);

    for v_month in 1..12
    loop
        v_source_table := format(
            'sbis_%s%s01_uploading',
            p_year,
            lpad(v_month::text, 2, '0')
        );
        v_source_tables := array_append(v_source_tables, v_source_table);
    end loop;

    call sbis_create_union_table(v_target_table, v_source_tables);

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
    v_monthly_source_tables text[] := '{}';
    v_weekly_source_tables text[] := array[
        'sbis_week_202410_end_first_week_uploading',
        'sbis_week_202410_end_s_week_uploading',
        'sbis_week_202410_end_t_week_uploading',
        'sbis_week_202410_end_f_week_uploading',
        'sbis_week_202410_end_r_week_uploading',
        'sbis_week_202411_end_first_week_uploading',
        'sbis_week_202411_end_s_week_uploading',
        'sbis_week_202411_end_t_week_uploading',
        'sbis_week_202411_end_f_week_uploading',
        'sbis_week_202411_end_r_week_uploading',
        'sbis_week_202412_end_first_week_uploading',
        'sbis_week_202412_end_s_week_uploading',
        'sbis_week_202412_end_t_week_uploading',
        'sbis_week_202412_end_f_week_uploading',
        'sbis_week_202412_end_r_week_uploading'
    ];
begin
    for v_month in 1..9
    loop
        v_source_table := format(
            'sbis_2024%s01_uploading',
            lpad(v_month::text, 2, '0')
        );
        v_monthly_source_tables := array_append(v_monthly_source_tables, v_source_table);
    end loop;

    call sbis_create_union_table('sbis_upl_2024_monthly', v_monthly_source_tables);

    for v_month in 1..9
    loop
        v_source_table := format(
            'sbis_2024%s01_uploading',
            lpad(v_month::text, 2, '0')
        );
        call sbis_append_table_if_exists('sbis_upl_2024_monthly', v_source_table);
    end loop;

    call sbis_create_union_table('sbis_upl_2024_weekly', v_weekly_source_tables);

    call sbis_append_week_month('sbis_upl_2024_weekly', '202410');
    call sbis_append_week_month('sbis_upl_2024_weekly', '202411');
    call sbis_append_week_month('sbis_upl_2024_weekly', '202412');

    call sbis_create_union_table(
        'sbis_upl_2024',
        array['sbis_upl_2024_monthly', 'sbis_upl_2024_weekly']
    );

    call sbis_append_table_if_exists('sbis_upl_2024', 'sbis_upl_2024_monthly');
    call sbis_append_table_if_exists('sbis_upl_2024', 'sbis_upl_2024_weekly');
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
    v_source_tables text[] := '{}';
begin
    v_target_table := format('sbis_upl_%s', p_year);

    for v_month in 1..12
    loop
        v_month_code := format(
            '%s%s',
            p_year,
            lpad(v_month::text, 2, '0')
        );
        v_source_tables := v_source_tables || array[
            format('sbis_week_%s_end_first_week_uploading', v_month_code),
            format('sbis_week_%s_end_s_week_uploading', v_month_code),
            format('sbis_week_%s_end_t_week_uploading', v_month_code),
            format('sbis_week_%s_end_f_week_uploading', v_month_code),
            format('sbis_week_%s_end_r_week_uploading', v_month_code)
        ];
    end loop;

    call sbis_create_union_table(v_target_table, v_source_tables);

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
    v_final_source_tables text[];
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

    v_final_source_tables := array[
        'sbis_upl_2020',
        'sbis_upl_2021',
        'sbis_upl_2022',
        'sbis_upl_2023',
        'sbis_upl_2024'
    ];

    if p_last_year >= 2025 then
        select v_final_source_tables || array_agg(format('sbis_upl_%s', gs))
        into v_final_source_tables
        from generate_series(2025, p_last_year) as gs;
    end if;

    call sbis_create_union_table(
        'sbis_upl_selling.sbis_upl_selling',
        v_final_source_tables
    );

    call sbis_append_table_if_exists('sbis_upl_selling.sbis_upl_selling', 'sbis_upl_2020');
    call sbis_append_table_if_exists('sbis_upl_selling.sbis_upl_selling', 'sbis_upl_2021');
    call sbis_append_table_if_exists('sbis_upl_selling.sbis_upl_selling', 'sbis_upl_2022');
    call sbis_append_table_if_exists('sbis_upl_selling.sbis_upl_selling', 'sbis_upl_2023');
    call sbis_append_table_if_exists('sbis_upl_selling.sbis_upl_selling', 'sbis_upl_2024');

    if p_last_year >= 2025 then
        for v_year in 2025..p_last_year
        loop
            call sbis_append_table_if_exists(
                'sbis_upl_selling.sbis_upl_selling',
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

-- --------------------------------------------------------
-- --------------------------------------------------------
-- --------------------------------------------------------

create or replace procedure create_truncate_table_sbis_upl_selling()
    language plpgsql
    as $$
        begin
            drop table if exists sbis_upl_selling_from_warehouse.sbis_upl_selling;
            create table sbis_upl_selling_from_warehouse.sbis_upl_selling as (select * from sbis_upl_selling_from_warehouse.columns_name_sbis_upl);
            alter table sbis_upl_selling_from_warehouse.sbis_upl_selling
            drop column level_0;

            truncate table sbis_upl_selling_from_warehouse.sbis_upl_selling; 
        

        end;
$$;


call create_truncate_table_sbis_upl_selling();


-- --------------------------------------------------------
-- --------------------------------------------------------
-- --------------------------------------------------------

create or replace procedure create_table_sbis_coll_sell()
    language plpgsql
    as $$
        begin
            drop table if exists intermediate_scheme.sbis_coll_sell;
            create table intermediate_scheme.sbis_coll_sell as (select * from sbis_upl_selling_from_warehouse.sbis_upl_selling);

            ALTER TABLE intermediate_scheme.sbis_coll_sell
            ADD COLUMN inside_unique_id UUID DEFAULT uuid_generate_v1();

            UPDATE intermediate_scheme.sbis_coll_sell
            set inside_unique_id = uuid_generate_v1()
            where inside_unique_id is NULL; 

        end;
$$;
call create_table_sbis_coll_sell();