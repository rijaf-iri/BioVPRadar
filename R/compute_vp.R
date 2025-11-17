#' Compute vertical profile.
#'
#' Compute a vertical profile of bird and insect from a polar volume.
#' 
#' @param radar_file character, full path to a single radar polar volume file.
#' The file data format should be \code{ODIM HDF5} or \code{NEXRAD Level 2}.
#' @param config_dir character, full path to BioConfigRadar.
#' @param rwanda logical, \code{TRUE} if the file is from MeteoRwanda new format \code{ODIM HDF5}.
#' @param radar_id integer, the radar id.
#'  
#' @export

compute_vp <- function(
    radar_file, config_dir,
    rwanda = TRUE,  radar_id = 1
){
    on.exit({
        closeDB(con)
        if(rwanda){
            unlink(tmp_pvol_file)
        }
    })

    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        stop('Unable to connect to BioDBRadar.\n')
    }

    if(rwanda){
        rwd_wrong_swp <- jsonlite::fromJSON(
            file.path(config_dir, 'config', 'config_rwanda_new_odim.json')
        )
        args <- c(pvol_file = radar_file, rwd_wrong_swp)
        pvol <- do.call(rwanda_read_pvol, args)
        tmp_pvol_file <- tempfile(fileext = '.h5')
        bioRad::write_pvolfile(
            pvol, file = tmp_pvol_file, overwrite = TRUE
        )
    }else{
        tmp_pvol_file <- radar_file
    }

    config_rcs <- jsonlite::fromJSON(
        file.path(config_dir, 'config', 'config_rcs.json')
    )
    config_user <- jsonlite::fromJSON(
        file.path(config_dir, 'config', 'config_vlo2bird.json')
    )
    config <- vol2birdR::vol2bird_config()

    polar_id <- NULL
    for(species in names(config_rcs)){
        config_user$birdRadarCrossSection <- config_rcs[[species]]
        config_species <- assign_lists(config_user, config)

        tmp_vp_file <- tempfile(fileext = '.h5')
        vol2birdR::vol2bird(
            file = tmp_pvol_file,
            config = config_species,
            vpfile = tmp_vp_file,
            verbose = FALSE
        )

        vp <- bioRad::read_vpts(tmp_vp_file)
        vp <- as.data.frame(vp)
        unlink(tmp_vp_file)

        if(is.null(polar_id)){
            vp_polar <- list(
                radar_id = radar_id,
                date_time = vp$datetime[1],
                rcs_bird = config_rcs$bird,
                rcs_insect = config_rcs$insect,
                sd_vvp_threshold = vp$sd_vvp_threshold[1],
                day_info = vp$day[1],
                sunrise = vp$sunrise[1],
                sunset = vp$sunset[1]
            )
            polar_id <- populate_vp_polar(con, vp_polar)
        }

        vp_species <- cbind(polar_id = polar_id, vp[, 3:18])
        table_name <- paste0('vp_', species)
        populate_vp_species(con, vp_species, table_name)
    }

    update_vp_timerange(con, radar_id, vp$datetime[1])
}

populate_vp_polar <- function(con, data){
    keys <- c(
        'radar_id', 'date_time',
        'rcs_bird', 'rcs_insect',
        'sd_vvp_threshold', 'day_info',
        'sunrise', 'sunset'
    )
    ik <- match(keys, names(data))
    if(any(is.na(ik))){
        miss <- paste0(
            keys[is.na(ik)], collapse = ', '
        )
        stop(
            paste(
                'Incomplete data columns.\n',
                paste('Missing:', miss, '\n')
            )
        )
    }

    sqlCmd <- "
        INSERT INTO vp_polar
          (radar_id, date_time, rcs_bird, rcs_insect,
           sd_vvp_threshold, day_info, sunrise, sunset)
        VALUES
          ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (radar_id, date_time)
        DO UPDATE SET
          rcs_bird = EXCLUDED.rcs_bird,
          rcs_insect = EXCLUDED.rcs_insect,
          sd_vvp_threshold = EXCLUDED.sd_vvp_threshold,
          day_info = EXCLUDED.day_info,
          sunrise = EXCLUDED.sunrise,
          sunset = EXCLUDED.sunset;
      "

    DBI::dbExecute(
        con, sqlCmd, params = unname(data[ik])
    )

    sqlCmd <- "
        SELECT id FROM vp_polar
        WHERE radar_id = $1 AND date_time = $2;
      "
    idx <- unname(data[c('radar_id', 'date_time')])
    row <- DBI::dbGetQuery(con, sqlCmd, params = idx)
    if (nrow(row) == 0) {
        stop('Unexpected error: UPSERT completed but no row found.')
    }

    return(row$id[1])
}

populate_vp_species <- function(con, data, table_name){
    keys <- c(
        'polar_id', 'height', 'ff', 'dbz', 'dens',
        'u', 'v', 'gap', 'w', 'n_dbz', 'dd', 'n',
        'DBZH', 'n_dbz_all', 'eta', 'sd_vvp', 'n_all'
    )
    ik <- match(keys, names(data))
    if(any(is.na(ik))){
        miss <- paste0(
            keys[is.na(ik)], collapse = ', '
        )
        stop(
            paste(
                'Incomplete data columns.\n',
                paste('Missing:', miss, '\n')
            )
        )
    }

    conv <- function(x){
        ifelse(is.na(x) | is.nan(x) | is.infinite(x), NA, x)
    }
    df <- as.data.frame(lapply(data[, ik], conv))

    sqlCmd <- "
        INSERT INTO %s
            (polar_id, height, ff, dbz, dens, u, v, gap, w,
             n_dbz, dd, n, DBZH, n_dbz_all, eta, sd_vvp, n_all)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9,
             $10, $11, $12, $13, $14, $15, $16, $17)
        ON CONFLICT (polar_id, height)
        DO UPDATE SET
            ff = EXCLUDED.ff,
            dbz = EXCLUDED.dbz,
            dens = EXCLUDED.dens,
            u = EXCLUDED.u,
            v = EXCLUDED.v,
            gap = EXCLUDED.gap,
            w = EXCLUDED.w,
            n_dbz = EXCLUDED.n_dbz,
            dd = EXCLUDED.dd,
            n = EXCLUDED.n,
            DBZH = EXCLUDED.DBZH,
            n_dbz_all = EXCLUDED.n_dbz_all,
            eta = EXCLUDED.eta,
            sd_vvp = EXCLUDED.sd_vvp,
            n_all = EXCLUDED.n_all;
        "
    sqlCmd <- sprintf(sqlCmd, table_name)

    for(i in seq_len(nrow(df))){
        params <- unname(as.list(df[i, ]))
        DBI::dbExecute(con, sqlCmd, params = params)
    }

    return(0)
}

update_vp_timerange <- function(con, radar_id, date_time){
    sqlCmd <- sprintf(
        "SELECT * FROM vp_timerange WHERE radar_id=%s",
        radar_id
    )
    time_range <- DBI::dbGetQuery(con, sqlCmd)
    if(nrow(time_range) > 0){
        update <- FALSE
        if(date_time < time_range$start_time){
            sqlCmd <- "
                UPDATE vp_timerange
                SET start_time = $1
                WHERE radar_id = $2;
               "
            update <- TRUE
        }
        if(date_time > time_range$end_time){
            sqlCmd <- "
                UPDATE vp_timerange
                SET end_time = $1
                WHERE radar_id = $2;
               "
            update <- TRUE
        }
        if(update){
            params <- list(date_time, radar_id)
            DBI::dbExecute(con, sqlCmd, params = params)
        }
    }else{
        sqlCmd <- "
            INSERT INTO vp_timerange 
              (radar_id, start_time, end_time)
            VALUES 
              ($1, $2, $3);
          "
        params <- list(
            radar_id, date_time, date_time
        )
        DBI::dbExecute(con, sqlCmd, params = params)
    }

    return(0)
}
