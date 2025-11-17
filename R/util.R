
assign_lists <- function(inupt_args, init_pars){
    pars_name <- names(init_pars)
    data_name <- names(inupt_args)
    inm <- data_name %in% pars_name
    if(any(inm)){
        for(n in data_name[inm])
            init_pars[[n]] <- inupt_args[[n]]
    }

    return(init_pars)
}
