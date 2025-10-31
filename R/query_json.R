get_vp_json <- function(vp, parameter){
    info <- get_vp_params(parameter)
    df_vp <- as.data.frame(vp)
    out <- list(time = df_vp$datetime[1], name = info$name, units = info$units,
                day = df_vp$day[1], sunrise = df_vp$sunrise[1], sunset = df_vp$sunset[1],
                height = df_vp$height, parameter = df_vp[, parameter],
                ff = df_vp$ff, dd = df_vp$dd)
    jsonlite::toJSON(out, na = 'null', auto_unbox = TRUE)
}

get_vpts_json <- function(vpts, parameter){
    info <- get_vp_params(parameter)
    df_vpts <- as.data.frame(vpts)

    ff <- reshape2::acast(df_vpts, height ~ datetime, value.var = 'ff')
    ff <- as.matrix(ff)
    dimnames(ff) <- NULL

    dd <- reshape2::acast(df_vpts, height ~ datetime, value.var = 'dd')
    dd <- as.matrix(dd)
    dimnames(dd) <- NULL

    param <- reshape2::acast(df_vpts, height ~ datetime, value.var = parameter)
    times <- dimnames(param)[[2]]
    height <- as.numeric(dimnames(param)[[1]])

    param <- as.matrix(param)
    dimnames(param) <- NULL

    it <- which(df_vpts$height == df_vpts$height[1])
    day <- df_vpts$day[it]
    sunrise <- df_vpts$sunrise[it]
    sunset <- df_vpts$sunset[it]

    out <- list(name = info$name, units = info$units,
                times = times, height = height, 
                parameter = param, ff = ff, dd = dd,
                sunrise = sunrise, sunset = sunset)

    jsonlite::toJSON(out, na = 'null', auto_unbox = TRUE)
}

get_vtip_json <- function(vpts, parameter){
    info <- get_vpi_params(parameter)
    vpi <- bioRad::integrate_profile(vpts)
    df_vpts <- as.data.frame(vpts)
    it <- which(df_vpts$height == df_vpts$height[1])
    day <- df_vpts$day[it]
    sunrise <- df_vpts$sunrise[it]
    sunset <- df_vpts$sunset[it]

    out <- list(name = info$name, units = info$units,
                height = vpi$height, times = vpi$datetime,
                parameter = vpi[, parameter], ff = vpi$ff, dd = vpi$dd,
                day = day, sunrise = sunrise, sunset = sunset)
    jsonlite::toJSON(out, na = 'null', auto_unbox = TRUE)
}
