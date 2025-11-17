#' Add new radar site.
#'
#' Populate \code{radar_table} with new radar.
#' 
#' @param config_dir character, full path to BioConfigRadar.
#' @param radar named list, list(name, longitude, latitude, altitude, wavelength)
#' 
#' @export

add_new_radar <- function(config_dir, radar){
    on.exit(closeDB(con))
    con <- bioDBRadar(config_dir)
    if(is.null(con)){
        stop('Unable to connect to BioDBRadar.\n')
    }

    keys <- c(
        'name', 'longitude', 'latitude',
        'altitude', 'wavelength'
    )

    ik <- match(keys, names(radar))
    if(any(is.na(ik))){
        miss <- paste0(
            keys[is.na(ik)], collapse = ', '
        )
        stop(
            paste(
                'Incomplete radar information.\n',
                paste('Missing:', miss, '\n')
            )
        )
    }

    sqlCmd <- "
        INSERT INTO radar_table
        (name, longitude, latitude, altitude, wavelength)
        VALUES ($1, $2, $3, $4, $5);
      "
     DBI::dbExecute(
        con, sqlCmd, params = unname(radar[ik])
    )

    return(0)
}
