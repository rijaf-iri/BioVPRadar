compute_vp <- function(
        pvol, vp_conf_file,
        nyquist_velocity = NULL
    )
{
    # pvol <- bioRad::read_pvolfile(radar_file)
    vp_conf <- jsonlite::fromJSON(vp_conf_file)
    if(!is.null(nyquist_velocity)){
        if(is.na(nyquist_velocity)){
            nyquist_velocity <- 15
        }
        for(j in seq_along(pvol$scans)){
            pvol$scans[[j]]$attributes$how$NI <- nyquist_velocity
        }
    }

    do.call(
        bioRad::calculate_vp,
        c(list(file = pvol), vp_conf)
    )
}
