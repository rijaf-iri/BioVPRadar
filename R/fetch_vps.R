get_vp_db <- function(config_dir, query){
    on.exit(closeDB(con))
    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        msg <- 'Unable to connect to BioDBRadar.'
        return(list(status = -1, message = msg))
    }
    ret <- try(
        fetch_vp(con,
            query$time,
            query$species,
            query$radarID
        ),
        silent = TRUE 
    )
    if(inherits(ret, 'try-error')){
        msg <- gsub('[\r\n]', '', ret[1])
        return(list(status = -1, message = msg))
    }

    return(ret)
}

get_vpts_db <- function(config_dir, query){
    on.exit(closeDB(con))
    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        msg <- 'Unable to connect to BioDBRadar.'
        return(list(status = -1, message = msg))
    }
    ret <- try(
        fetch_vpts(con,
            query$startTime,
            query$endTime,
            query$species,
            query$radarID
        ),
        silent = TRUE 
    )
    if(inherits(ret, 'try-error')){
        msg <- gsub('[\r\n]', '', ret[1])
        return(list(status = -1, message = msg))
    }

    return(ret)
}

fetch_vp <- function(con, time, species, radar_id){
    radar <- DBI::dbGetQuery(con,
        "SELECT * FROM radar_table WHERE id=$1;",
        params = list(radar_id)
    )
    radar <- radar[, -1]
    names(radar) <- c(
        'radar', 'radar_latitude', 'radar_longitude',
        'radar_height', 'radar_wavelength'
    )

    params <- list(radar_id, time)

    vp_p <- DBI::dbGetQuery(con, 
        "SELECT * FROM vp_polar
         WHERE radar_id=$1 AND date_time=$2;",
        params = params
    )
    if(nrow(vp_p) == 0){
        msg <- 'No data found'
        return(list(status = -1, message = msg))
    }

    col_p <- c(
        'date_time', paste0('rcs_', species),
        'sd_vvp_threshold', 'day_info',
        'sunrise', 'sunset'
    )
    vp_p <- vp_p[, col_p]
    names(vp_p) <- c(
        'datetime', 'rcs', 'sd_vvp_threshold',
        'day', 'sunrise', 'sunset'
    )

    sqlCmd <- sprintf(
        "SELECT s.*
         FROM %s s
         JOIN vp_polar p ON s.polar_id=p.id
         WHERE p.radar_id=$1 AND p.date_time=$2;",
        paste0('vp_', species)
    )
    vp_s <- DBI::dbGetQuery(
        con, sqlCmd, params = params
    )
    vp_s <- vp_s[, -(1:2)]
    names(vp_s)[names(vp_s) == 'dbzh'] <- 'DBZH'

    rm <- c('day', 'sunrise', 'sunset')
    vp <- cbind(vp_s, vp_p, radar)
    vp_p <- vp[, rm]
    vp <- vp[, !names(vp) %in% rm]
    vp <- bioRad::as.vp(vp)
    vp$data <- cbind(vp$data, vp_p)
    return(list(status = 0, vp = vp))
}

fetch_vpts <- function(
    con, start_time, end_time,
    species, radar_id
){
    radar <- DBI::dbGetQuery(con,
        "SELECT * FROM radar_table WHERE id=$1;",
        params = list(radar_id)
    )
    radar <- radar[, -1]
    names(radar) <- c(
        'radar', 'radar_latitude', 'radar_longitude',
        'radar_height', 'radar_wavelength'
    )

    params <- list(radar_id, start_time, end_time)

    vp_p <- DBI::dbGetQuery(con, 
        "SELECT * FROM vp_polar
         WHERE radar_id=$1 AND
         date_time>=$2 AND date_time<=$3;",
        params = params
    )
    if(nrow(vp_p) == 0){
        msg <- 'No data found'
        return(list(status = -1, message = msg))
    }

    col_p <- c(
        'id', 'date_time',
        paste0('rcs_', species),
        'sd_vvp_threshold', 'day_info',
        'sunrise', 'sunset'
    )
    vp_p <- vp_p[, col_p]
    names(vp_p) <- c(
        'id', 'datetime', 'rcs',
        'sd_vvp_threshold', 'day',
        'sunrise', 'sunset'
    )

    sqlCmd <- sprintf(
        "SELECT s.*
         FROM %s s
         JOIN vp_polar p ON s.polar_id=p.id
         WHERE p.radar_id=$1 AND
         p.date_time>=$2 AND p.date_time<=$3;",
        paste0('vp_', species)
    )
    vp_s <- DBI::dbGetQuery(
        con, sqlCmd, params = params
    )
    vp_s <- vp_s[, -1]
    names(vp_s)[names(vp_s) == 'dbzh'] <- 'DBZH'

    rm <- c('day', 'sunrise', 'sunset')
    vps <- lapply(vp_p$id, function(id){
        tmp_p <- vp_p[vp_p$id == id, -1]
        tmp_s <- vp_s[vp_s$polar_id == id, -1]
        tmp <- cbind(tmp_s, tmp_p, radar)
        tmp_p <- tmp[, rm]
        tmp <- tmp[, !names(tmp) %in% rm]
        tmp <- bioRad::as.vp(tmp)
        tmp$data <- cbind(tmp$data, tmp_p)
        tmp
    })

    return(list(status = 0, vp = vps))
}
