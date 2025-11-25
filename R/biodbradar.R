#' Create the authentication to BioDBRadar.
#'
#' Create the authentication to BioDBRadar.
#' 
#' @param config_dir character, full path to \code{BioConfigRadar}.
#' @param db_name character, PostgreSQL database name.
#' @param username character, PostgreSQL user name.
#' @param password character, PostgreSQL user password.
#' @param host_name character, the host name, default \code{"localhost"}.
#' @param file_name character, the file name of the connection, default \code{"biodb.con"}.
#' 
#' @export

create_bioDBConnection <- function(
    config_dir, db_name,
    username, password, 
    host_name = 'localhost',
    file_name = 'biodb.con'
){
    config_list <- list(
        dbname = db_name,
        user = username,
        password = password,
        host = host_name,
        port = 5432,
        bigint = 'integer64'
    )
    con_args <- new.env()
    con_args$connection <- config_list
    fauth <- file.path(config_dir, 'auth', file_name)
    saveRDS(con_args, fauth)
    return(0)
}

#' Create BioDBRadar tables.
#'
#' Create the tables of BioDBRadar.
#' 
#' @param config_dir character, full path to BioConfigRadar.
#' 
#' @export

create_bioDBRadar <- function(config_dir){
    on.exit(closeDB(con))
    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        stop('Unable to connect to BioDBRadar.\n')
    }

    create_radar_table(con)
    create_vp_polar(con)
    create_vp_species(con, 'vp_bird')
    create_vp_species(con, 'vp_insect')
    create_vp_timerange(con)
    create_vid_timerange(con)
    create_rgrid_timerange(con)
    create_rpolar_timerange(con)
    create_biometeo_timerange(con)
    create_bioclass_timerange(con)

    return(0)
}

#' Verify BioDBRadar tables.
#'
#' Check the tables and indexes of BioDBRadar.
#' 
#' @param config_dir character, full path to BioConfigRadar.
#' 
#' @export

verify_bioDBRadar <- function(config_dir) {
    on.exit(closeDB(con))
    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        stop('Unable to connect to BioDBRadar.\n')
    }

    message('\n====== BioDBRadar SCHEMA VERIFICATION ======')

    # Check tables
    tables <- DBI::dbGetQuery(con, "
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
        ORDER BY table_name;
    ")

    # Check indexes
    indexes <- DBI::dbGetQuery(con, "
        SELECT tablename, indexname
        FROM pg_indexes
        WHERE schemaname = 'public'
        ORDER BY tablename;
    ")

    # Check constraints
    constraints <- DBI::dbGetQuery(con, "
        SELECT constraint_name, table_name
        FROM information_schema.table_constraints
        WHERE constraint_type IN ('PRIMARY KEY','UNIQUE','FOREIGN KEY');
    ")

    ###
    # Table verification
    message('\nExisting Tables:')
    print(tables)

    expected_tables <- c(
        'radar_table', 'vp_polar',
        'vp_bird', 'vp_insect',
        'vp_timerange', 'vid_timerange',
        'rgrid_timerange', 'rpolar_timerange',
        'biometeo_timerange', 'bioclass_timerange'
    )

    missing_tbl <- setdiff(
        expected_tables, tables$table_name
    )
    if (length(missing_tbl) == 0) {
        message('All expected tables exist.')
    } else {
        warning(
            'Missing tables: ',
            paste(missing_tbl, collapse = ', ')
        )
    }

    # Index verification
    message('\nExisting Indexes:')
    print(indexes)

    expected_indexes <- c(
        'idx_vp_polar_radar_time',
        'idx_vp_bird_polar',
        'idx_vp_insect_polar'
    )

    missing_idx <- setdiff(
        expected_indexes, indexes$indexname
    )
    if (length(missing_idx) == 0) {
        message('All expected indexes exist.')
    } else {
        warning(
            'Missing indexes: ',
            paste(missing_idx, collapse = ', ')
        )
    }

    # Constraints verification
    message('\nExisting Constraints:')
    print(constraints)

    message('\nVerification complete.')
    message('===================================================\n')
}

create_radar_table <- function(con){
    createDBTable(
        con, 'radar_table', TRUE,
        'id SERIAL PRIMARY KEY',
        'name TEXT',
        'longitude REAL',
        'latitude REAL',
        'altitude REAL',
        'wavelength REAL'
    )
 
    return(0)
}

create_vp_polar <- function(con){
    createDBTable(
        con, 'vp_polar', TRUE,
        'id SERIAL PRIMARY KEY',
        'radar_id INT REFERENCES radar_table(id)',
        'date_time TIMESTAMP NOT NULL',
        'rcs_bird REAL',
        'rcs_insect REAL',
        'sd_vvp_threshold REAL',
        'day_info BOOLEAN',
        'sunrise TIMESTAMP',
        'sunset TIMESTAMP',
        'UNIQUE (radar_id, date_time)'
    )

    DBI::dbExecute(con, "
        CREATE INDEX IF NOT EXISTS
        idx_vp_polar_radar_time
        ON vp_polar (radar_id, date_time)
    ")

    return(0)
}

create_vp_species <- function(con, table_name){
    createDBTable(
        con, table_name, TRUE,
        'id SERIAL PRIMARY KEY',
        'polar_id INT REFERENCES vp_polar(id) ON DELETE CASCADE',
        'height REAL',
        'ff REAL',
        'dbz REAL',
        'dens REAL',
        'u REAL',
        'v REAL',
        'gap BOOLEAN',
        'w REAL',
        'n_dbz REAL',
        'dd REAL',
        'n INT',
        'DBZH REAL',
        'n_dbz_all INT',
        'eta REAL',
        'sd_vvp REAL',
        'n_all INT',
        'UNIQUE (polar_id, height)'
    )

    sqlCmd <- sprintf(
        "CREATE INDEX IF NOT EXISTS
         idx_%s_polar ON %s (polar_id)",
        table_name, table_name
    )
    DBI::dbExecute(con, sqlCmd)

    return(0)
}

create_vp_timerange <- function(con){
    createDBTable(
        con, 'vp_timerange', FALSE,
        'radar_id INT NOT NULL',
        'start_time TIMESTAMP NOT NULL',
        'end_time TIMESTAMP NOT NULL'
    )

    return(0)
}

create_vid_timerange <- function(con){
    createDBTable(
        con, 'vid_timerange', FALSE,
        'radar_id INT NOT NULL',
        'start_time TIMESTAMP NOT NULL',
        'end_time TIMESTAMP NOT NULL'
    )

    return(0)
}

create_rgrid_timerange <- function(con){
    createDBTable(
        con, 'rgrid_timerange', FALSE,
        'radar_id INT NOT NULL',
        'start_time TIMESTAMP NOT NULL',
        'end_time TIMESTAMP NOT NULL'
    )

    return(0)
}

create_rpolar_timerange <- function(con){
    createDBTable(
        con, 'rpolar_timerange', FALSE,
        'radar_id INT NOT NULL',
        'start_time TIMESTAMP NOT NULL',
        'end_time TIMESTAMP NOT NULL'
    )

    return(0)
}

create_biometeo_timerange <- function(con){
    createDBTable(
        con, 'biometeo_timerange', FALSE,
        'radar_id INT NOT NULL',
        'start_time TIMESTAMP NOT NULL',
        'end_time TIMESTAMP NOT NULL'
    )

    return(0)
}

create_bioclass_timerange <- function(con){
    createDBTable(
        con, 'bioclass_timerange', FALSE,
        'radar_id INT NOT NULL',
        'start_time TIMESTAMP NOT NULL',
        'end_time TIMESTAMP NOT NULL'
    )

    return(0)
}
