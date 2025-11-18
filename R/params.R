get_vp_params <- function(parameter){
    parameters <- list(
        list(parameter = 'dens', name = 'Volume density', units = '#/km3'),
        list(parameter = 'eta', name = 'Reflectivity eta', units = 'cm2/km3'),
        list(parameter = 'dbz', name = 'Reflectivity factor', units = 'dBZe'),
        list(parameter = 'w', name = 'Vertical velocity', units = 'm/s'),
        list(parameter = 'n_dbz', name = 'Number of range gates in density estimates', units = '#'),
        list(parameter = 'n_dbz_all', name = 'Number of range gates in DBZH estimates', units = '#'),
        list(parameter = 'sd_vvp', name = 'VVP-retrieved radial velocity stdev', units = 'm/s')
    )
    params <- sapply(parameters, '[[', 'parameter')
    ip <- which(params == parameter)
    parameters[[ip]]
}

get_vpi_params <- function(parameter){
    parameters <- list(
        list(parameter = 'vid', name = 'Vertically Integrated Densities', units = '#/km2'),
        list(parameter = 'vir', name = 'Vertically Integrated Reflectivity', units = 'cm2/km2'),
        list(parameter = 'mtr', name = 'Migration Traffic Rate', units = '#/km/h'),
        list(parameter = 'rtr', name = 'Reflectivity Traffic Rate', units = 'cm2/km/h'),
        list(parameter = 'mt', name = 'Cumulative Migration Traffic', units = '#/km'),
        list(parameter = 'rt', name = 'Cumulative Reflectivity Traffic', units = 'cm2/km')
    )
    params <- sapply(parameters, '[[', 'parameter')
    ip <- which(params == parameter)
    parameters[[ip]]
}
