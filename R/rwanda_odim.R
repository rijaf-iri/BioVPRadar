
remove_invalid_scans <- function(pvol, nbins = NULL, elangle = NULL){
    if(is.null(nbins) | is.null(elangle)) return(pvol)
    scans_nbins <- sapply(pvol$scans, function(x) x$attributes$where$nbins)
    scans_elangle <- sapply(pvol$scans, function(x) x$attributes$where$elangle)
    rm <- scans_nbins %in% nbins & scans_elangle %in% elangle
    pvol$scans <- pvol$scans[!rm]
    return(pvol)
}

correct_attrs_scans <- function(pvol){
    pvol$attributes$how$beamwH <- as.numeric(pvol$attributes$how$beamwH)
    pvol$attributes$how$beamwV <- as.numeric(pvol$attributes$how$beamwV)
    pvol$attributes$how$beamwidth <- as.numeric(pvol$attributes$how$beamwidth)

    sweeps <- bioRad::get_elevation_angles(pvol)
    nsweep <- length(sweeps)
    if(is.null(pvol$attributes$how$scan_count)){
        pvol$attributes$how$scan_count <- nsweep
    }

    for(j in seq(nsweep)){
        pvol$scans[[j]]$attributes$how$scan_count <- nsweep
        pvol$scans[[j]]$attributes$how$scan_index <- j
        pvol$scans[[j]]$attributes$where$rstart <- pvol$scans[[j]]$geo$rstart

        for(p in seq_along(pvol$scans[[j]]$params)){
            attributes(pvol$scans[[j]]$params[[p]])$conversion$nodata <- -9999
            attributes(pvol$scans[[j]]$params[[p]])$conversion$undetect <- -999
        }
    }

    return(pvol)
}

rwanda_read_pvol <- function(pvol_file, nbins, elangle, bio_filter = TRUE){
    pvol <- bioRad::read_pvolfile(pvol_file)
    pvol <- remove_invalid_scans(pvol, nbins, elangle)
    pvol <- correct_attrs_scans(pvol)
    if(bio_filter){
        pvol <- filter_non_biological(pvol)
    }
    return(pvol)
}

compute_dr <- function(ZDR, RHOHV){
    num <- 1 + ZDR - 2 * (ZDR^0.5) * RHOHV
    den <- 1 + ZDR + 2 * (ZDR^0.5) * RHOHV
    dr <- suppressWarnings(10 * log10(num / den))
    return(dr)
}

filter_non_biological <- function(pvol){
    DBZH <- DR <- ZDR <- RHOHV <- NA
    WRADH <- VRADH <- PHIDP <- NA
    pvol <- bioRad::calculate_param(
        pvol, 
        DBZH = dplyr::if_else(
            c(DBZH) > 15, NA, c(DBZH)
        )
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        DBZH = dplyr::if_else(
            c(RHOHV) > 0.90, NA, c(DBZH)
        )
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        DR = compute_dr(ZDR, RHOHV)
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        DBZH = dplyr::if_else(
            c(DR) < -12, NA, c(DBZH)
        )
    )
    # filter other fields
    pvol <- bioRad::calculate_param(
        pvol, 
        RHOHV = dplyr::if_else(
            c(is.na(DBZH)), NA, c(RHOHV)
        )
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        DR = dplyr::if_else(
            c(is.na(DBZH)), NA, c(DR)
        )
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        WRADH = dplyr::if_else(
            c(is.na(DBZH)), NA, c(WRADH)
        )
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        PHIDP = dplyr::if_else(
            c(is.na(DBZH)), NA, c(PHIDP)
        )
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        ZDR = dplyr::if_else(
            c(is.na(DBZH)), NA, c(ZDR)
        )
    )
    pvol <- bioRad::calculate_param(
        pvol, 
        VRADH = dplyr::if_else(
            c(is.na(DBZH)), NA, c(VRADH)
        )
    )
    return(pvol)
}
