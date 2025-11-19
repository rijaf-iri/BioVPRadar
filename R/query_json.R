get_vp_json <- function(vp, query){
    info <- get_vp_params(query$parameter)
    df_vp <- as.data.frame(vp)
    frmt <- '%Y-%m-%d %H:%M:%S'
    out <- list(
        time = format(df_vp$datetime[1], frmt),
        name = info$name,
        units = info$units,
        day = df_vp$day[1],
        sunrise = format(df_vp$sunrise[1], frmt),
        sunset = format(df_vp$sunset[1], frmt),
        height = df_vp$height,
        parameter = df_vp[, query$parameter],
        ff = df_vp$ff,
        dd = df_vp$dd,
        query_par = query$parameter,
        query_spec = query$species
    )
    jsonlite::toJSON(
        out, na = 'null', auto_unbox = TRUE
    )
}

get_vpts_json <- function(vpts, query){
    info <- get_vp_params(query$parameter)
    df_vpts <- as.data.frame(vpts)

    ff <- reshape2::acast(
        df_vpts, height ~ datetime, value.var = 'ff'
    )
    ff <- as.matrix(ff)
    dimnames(ff) <- NULL

    dd <- reshape2::acast(
        df_vpts, height ~ datetime, value.var = 'dd'
    )
    dd <- as.matrix(dd)
    dimnames(dd) <- NULL

    param <- reshape2::acast(
        df_vpts, height ~ datetime, value.var = query$parameter
    )
    times <- dimnames(param)[[2]]
    height <- as.numeric(dimnames(param)[[1]])

    param <- as.matrix(param)
    dimnames(param) <- NULL

    it <- which(df_vpts$height == df_vpts$height[1])
    day <- df_vpts$day[it]
    sunrise <- df_vpts$sunrise[it]
    sunset <- df_vpts$sunset[it]

    frmt <- '%Y-%m-%d %H:%M:%S'
    out <- list(
        name = info$name,
        units = info$units,
        times = format(times, frmt),
        height = height, 
        parameter = param,
        ff = ff,
        dd = dd,
        sunrise = format(sunrise, frmt),
        sunset = format(sunset, frmt),
        query_par = query$parameter,
        query_spec = query$species
    )
    jsonlite::toJSON(
        out, na = 'null', auto_unbox = TRUE
    )
}

get_vtip_json <- function(vpts, query){
    info <- get_vpi_params(query$parameter)
    vpi <- bioRad::integrate_profile(vpts)
    df_vpts <- as.data.frame(vpts)
    it <- which(df_vpts$height == df_vpts$height[1])
    day <- df_vpts$day[it]
    sunrise <- df_vpts$sunrise[it]
    sunset <- df_vpts$sunset[it]

    frmt <- '%Y-%m-%d %H:%M:%S'
    out <- list(
        name = info$name,
        units = info$units,
        height = vpi$height,
        times = format(vpi$datetime, frmt),
        parameter = vpi[, query$parameter],
        ff = vpi$ff,
        dd = vpi$dd,
        day = day,
        sunrise = format(sunrise, frmt),
        sunset = format(sunset, frmt),
        query_par = query$parameter,
        query_spec = query$species
    )
    jsonlite::toJSON(
        out, na = 'null', auto_unbox = TRUE
    )
}
