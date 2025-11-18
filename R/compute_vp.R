#' Compute vertical profile.
#'
#' Compute a vertical profile of bird and insect from a polar volume.
#' 
#' @param radar_file character, full path to a single radar polar volume file.
#' The file data format should be \code{ODIM HDF5} or \code{NEXRAD Level 2}.
#' @param config_dir character, full path to \code{BioConfigRadar}.
#' @param species character, \code{"bird"} or \code{"insect"}.
#' @param rwanda logical, \code{TRUE} if the file is from MeteoRwanda new format \code{ODIM HDF5}.
#' 
#' @return A vertical profile object of class \code{vp}.
#'  
#' @export

compute_vp <- function(
    radar_file, config_dir, species, rwanda = TRUE
){
    tmp_vp_file <- NULL
    tmp_pvol_file <- NULL
    on.exit({
        if(rwanda){
            unlink(tmp_pvol_file)
        }
        unlink(tmp_vp_file)
    })

    dir_config <- file.path(config_dir, 'config')

    if(rwanda){
        sweep_file <- file.path(dir_config, 'config_rwanda_new_odim.json')
        if(!file.exists(sweep_file)){
            stop(paste('File does not exist:', sweep_file))
        }
        rwd_wrong_swp <- jsonlite::fromJSON(sweep_file)
        args <- c(pvol_file = radar_file, rwd_wrong_swp)
        pvol <- do.call(rwanda_read_pvol, args)
        tmp_pvol_file <- tempfile(fileext = '.h5')
        bioRad::write_pvolfile(
            pvol, file = tmp_pvol_file, overwrite = TRUE
        )
    }else{
        tmp_pvol_file <- radar_file
    }

    rcs_file <- file.path(dir_config, 'config_rcs.json')
    if(!file.exists(rcs_file)){
        stop(paste('File does not exist:', rcs_file))
    }
    config_rcs <- jsonlite::fromJSON(rcs_file)

    vp_conf_file <- file.path(dir_config, 'config_vlo2bird.json')
    if(!file.exists(vp_conf_file)){
        stop(paste('File does not exist:', vp_conf_file))
    }
    config_user <- jsonlite::fromJSON(vp_conf_file)

    config <- vol2birdR::vol2bird_config()

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
    return(vpts_to_vp(vp))
}

vpts_to_vp <- function (x){
    stopifnot(inherits(x, 'vpts'))
    stopifnot(length(x$datetime) == 1)
    vpout <- list()
    vpout$radar <- x$radar
    vpout$datetime <- x$datetime[1]
    vpout$data <- as.data.frame(
        lapply(names(x$data), function(y) {
            x$data[y][[1]]
        })
    )
    names(vpout$data) <- names(x$data)
    vpout$attributes <- x$attributes
    vpout$data$height <- x$height
    class(vpout) <- 'vp'
    return(vpout)
}