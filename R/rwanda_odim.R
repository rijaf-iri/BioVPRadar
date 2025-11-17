
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

rwanda_read_pvol <- function(pvol_file, nbins, elangle){
    pvol <- bioRad::read_pvolfile(pvol_file)
    pvol <- remove_invalid_scans(pvol, nbins, elangle)
    pvol <- correct_attrs_scans(pvol)

    return(pvol)
}
