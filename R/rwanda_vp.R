#' Process vertical profile for MeteoRwanda radar data.
#'
#' Compute a vertical profile of bird and insect
#' from MeteoRwanda new format \code{ODIM HDF5}
#' and store the outputs into \code{BioDBRadar}.
#' 
#' @param radar_file character, full path to a single radar polar volume file.
#' The file data format should be \code{ODIM HDF5} or \code{NEXRAD Level 2}.
#' @param bioradar_dir character, full path to the parent folder containing \code{BioConfigRadar}.
#' @param radar_id integer, the radar id.
#'  
#' @export

wrapper_rwanda_vp <- function(
    radar_file, bioradar_dir, radar_id = 1
){
    tmp_vp_file <- NULL
    tmp_pvol_file <- NULL
    on.exit({
        closeDB(con)
        unlink(tmp_pvol_file)
        unlink(tmp_vp_file)
    })

    log_file <- get_log_file(
        bioradar_dir, 'logs_vp', 'vp'
    )
    config_dir <- file.path(
        bioradar_dir, 'BioConfigRadar'
    )

    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        msg <- 'Unable to connect to BioDBRadar.'
        format_out_msg(msg, log_file)
        return(-1)
    }

    dir_config <- file.path(config_dir, 'config')
    sweep_file <- file.path(
        dir_config, 'config_rwanda_new_odim.json'
    )
    if(!file.exists(sweep_file)){
        msg <- paste(
            'File does not exist:', sweep_file
        )
        format_out_msg(msg, log_file)
        return(-1)
    }
    rcs_file <- file.path(
        dir_config, 'config_rcs.json'
    )
    if(!file.exists(rcs_file)){
        msg <- paste(
            'File does not exist:', rcs_file
        )
        format_out_msg(msg, log_file)
        return(-1)
    }
    vp_conf_file <- file.path(
        dir_config, 'config_vlo2bird.json'
    )
    if(!file.exists(vp_conf_file)){
        msg <- paste(
            'File does not exist:', vp_conf_file
        )
        format_out_msg(msg, log_file)
        return(-1)
    }
    rwd_wrong_swp <- jsonlite::fromJSON(sweep_file)
    args <- c(pvol_file = radar_file, rwd_wrong_swp)
    pvol <- do.call(rwanda_read_pvol, args)
    tmp_pvol_file <- tempfile(fileext = '.h5')
    bioRad::write_pvolfile(
        pvol, file = tmp_pvol_file, overwrite = TRUE
    )
    rm(pvol)

    config_rcs <- jsonlite::fromJSON(rcs_file)
    config_user <- jsonlite::fromJSON(vp_conf_file)
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
            ret_vp_polar <- populate_vp_polar(con, vp_polar)
            if(is.null(ret_vp_polar$id)){
                format_out_msg(ret_vp_polar$msg, log_file)
                return(-1)
            }
            polar_id <- ret_vp_polar$id
        }

        vp_species <- cbind(polar_id = polar_id, vp[, 3:18])
        table_name <- paste0('vp_', species)
        ret_vp_species <- populate_vp_species(
            con, vp_species, table_name
        )
        if(is.null(ret_vp_species$status)){
            format_out_msg(ret_vp_species$msg, log_file)
            return(-1)
        }
    }

    updateTimeRangeTable(
        con, 'vp_timerange', radar_id, vp$datetime[1]
    )

    gc()
    return(0)
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
        miss <- paste0(keys[is.na(ik)], collapse = ', ')
        msg <- paste(
            'Incomplete data columns.\n',
            paste('Missing:', miss)
        )
        return(list(msg = msg))
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
        msg <- 'Unexpected error: UPSERT completed but no row found.'
        return(list(msg = msg))
    }

    return(list(id = row$id[1]))
}

populate_vp_species <- function(con, data, table_name){
    keys <- c(
        'polar_id', 'height', 'ff', 'dbz', 'dens',
        'u', 'v', 'gap', 'w', 'n_dbz', 'dd', 'n',
        'DBZH', 'n_dbz_all', 'eta', 'sd_vvp', 'n_all'
    )
    ik <- match(keys, names(data))
    if(any(is.na(ik))){
        miss <- paste0(keys[is.na(ik)], collapse = ', ')
        msg <- paste(
            'Incomplete data columns.\n',
            paste('Missing:', miss)
        )
        return(list(msg = msg))
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

    return(list(status = 0))
}
