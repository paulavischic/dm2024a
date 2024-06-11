#!/usr/bin/env Rscript

# Experimentos Colaborativos Default
# Workflow  Catastrophe Analysis

# limpio la memoria
rm(list = ls(all.names = TRUE)) # remove all objects
gc(full = TRUE) # garbage collection

require("data.table")
require("yaml")
require("mice")
require("lightgbm")
require("parallel")

#cargo la libreria
# args <- c( "~/dm2024a" )
args <- commandArgs(trailingOnly=TRUE)
source( paste0( args[1] , "/src/lib/action_lib.r" ) )
#------------------------------------------------------------------------------

CorregirCampoMes <- function(pcampo, pmeses) {
  
  tbl <- dataset[, list(
    "v1" = shift(get(pcampo), 1, type = "lag"),
    "v2" = shift(get(pcampo), 1, type = "lead")
  ),
  by = eval(envg$PARAM$dataset_metadata$entity_id)
  ]
  
  tbl[, paste0(envg$PARAM$dataset_metadata$entity_id) := NULL]
  tbl[, promedio := rowMeans(tbl, na.rm = TRUE)]
  
  dataset[
    ,
    paste0(pcampo) := ifelse(!(foto_mes %in% pmeses),
                             get(pcampo),
                             tbl$promedio
    )
  ]
}
#------------------------------------------------------------------------------
# reemplaza cada variable ROTA  (variable, foto_mes)
#  con el promedio entre  ( mes_anterior, mes_posterior )

Corregir_EstadisticaClasica <- function(dataset) {
  cat( "inicio Corregir_EstadisticaClasica()\n")
  
  CorregirCampoMes("thomebanking", c(201801, 202006))
  CorregirCampoMes("chomebanking_transacciones", c(201801, 201910, 202006))
  CorregirCampoMes("tcallcenter", c(201801, 201806, 202006))
  CorregirCampoMes("ccallcenter_transacciones", c(201801, 201806, 202006))
  CorregirCampoMes("cprestamos_personales", c(201801, 202006))
  CorregirCampoMes("mprestamos_personales", c(201801, 202006))
  CorregirCampoMes("mprestamos_hipotecarios", c(201801, 202006))
  CorregirCampoMes("ccajas_transacciones", c(201801, 202006))
  CorregirCampoMes("ccajas_consultas", c(201801, 202006))
  CorregirCampoMes("ccajas_depositos", c(201801, 202006))
  CorregirCampoMes("ccajas_extracciones", c(201801, 202006))
  CorregirCampoMes("ccajas_otras", c(201801, 202006))
  
  CorregirCampoMes("ctarjeta_visa_debitos_automaticos", c(201904))
  CorregirCampoMes("mttarjeta_visa_debitos_automaticos", c(201904, 201905))
  CorregirCampoMes("Visa_mfinanciacion_limite", c(201904))
  
  CorregirCampoMes("mrentabilidad", c(201905, 201910, 202006))
  CorregirCampoMes("mrentabilidad_annual", c(201905, 201910, 202006))
  CorregirCampoMes("mcomisiones", c(201905, 201910, 202006))
  CorregirCampoMes("mpasivos_margen", c(201905, 201910, 202006))
  CorregirCampoMes("mactivos_margen", c(201905, 201910, 202006))
  CorregirCampoMes("ccomisiones_otras", c(201905, 201910, 202006))
  CorregirCampoMes("mcomisiones_otras", c(201905, 201910, 202006))
  
  CorregirCampoMes("ctarjeta_visa_descuentos", c(201910))
  CorregirCampoMes("ctarjeta_master_descuentos", c(201910))
  CorregirCampoMes("mtarjeta_visa_descuentos", c(201910))
  CorregirCampoMes("mtarjeta_master_descuentos", c(201910))
  CorregirCampoMes("ccajeros_propios_descuentos", c(201910))
  CorregirCampoMes("mcajeros_propios_descuentos", c(201910))
  
  CorregirCampoMes("cliente_vip", c(201911))
  
  CorregirCampoMes("active_quarter", c(202006))
  CorregirCampoMes("mcuentas_saldo", c(202006))
  CorregirCampoMes("ctarjeta_debito_transacciones", c(202006))
  CorregirCampoMes("mautoservicio", c(202006))
  CorregirCampoMes("ctarjeta_visa_transacciones", c(202006))
  CorregirCampoMes("ctarjeta_visa_transacciones", c(202006))
  CorregirCampoMes("cextraccion_autoservicio", c(202006))
  CorregirCampoMes("mextraccion_autoservicio", c(202006))
  CorregirCampoMes("ccheques_depositados", c(202006))
  CorregirCampoMes("mcheques_depositados", c(202006))
  CorregirCampoMes("mcheques_emitidos", c(202006))
  CorregirCampoMes("mcheques_emitidos", c(202006))
  CorregirCampoMes("ccheques_depositados_rechazados", c(202006))
  CorregirCampoMes("mcheques_depositados_rechazados", c(202006))
  CorregirCampoMes("ccheques_emitidos_rechazados", c(202006))
  CorregirCampoMes("mcheques_emitidos_rechazados", c(202006))
  CorregirCampoMes("catm_trx", c(202006))
  CorregirCampoMes("matm", c(202006))
  CorregirCampoMes("catm_trx_other", c(202006))
  CorregirCampoMes("matm_other", c(202006))
  CorregirCampoMes("cmobile_app_trx", c(202006))
  
  cat( "fin Corregir_EstadisticaClasica()\n")
}
#------------------------------------------------------------------------------

Corregir_MachineLearning <- function(dataset) {
  gc()
  cat( "inicio Corregir_MachineLearning()\n")
  # acomodo los errores del dataset
  
  dataset[foto_mes == 201901, ctransferencias_recibidas := NA]
  dataset[foto_mes == 201901, mtransferencias_recibidas := NA]
  
  dataset[foto_mes == 201902, ctransferencias_recibidas := NA]
  dataset[foto_mes == 201902, mtransferencias_recibidas := NA]
  
  dataset[foto_mes == 201903, ctransferencias_recibidas := NA]
  dataset[foto_mes == 201903, mtransferencias_recibidas := NA]
  
  dataset[foto_mes == 201904, ctransferencias_recibidas := NA]
  dataset[foto_mes == 201904, mtransferencias_recibidas := NA]
  dataset[foto_mes == 201904, ctarjeta_visa_debitos_automaticos := NA]
  dataset[foto_mes == 201904, mttarjeta_visa_debitos_automaticos := NA]
  dataset[foto_mes == 201904, Visa_mfinanciacion_limite := NA]
  
  dataset[foto_mes == 201905, ctransferencias_recibidas := NA]
  dataset[foto_mes == 201905, mtransferencias_recibidas := NA]
  dataset[foto_mes == 201905, mrentabilidad := NA]
  dataset[foto_mes == 201905, mrentabilidad_annual := NA]
  dataset[foto_mes == 201905, mcomisiones := NA]
  dataset[foto_mes == 201905, mpasivos_margen := NA]
  dataset[foto_mes == 201905, mactivos_margen := NA]
  dataset[foto_mes == 201905, ctarjeta_visa_debitos_automaticos := NA]
  dataset[foto_mes == 201905, ccomisiones_otras := NA]
  dataset[foto_mes == 201905, mcomisiones_otras := NA]
  
  dataset[foto_mes == 201910, mpasivos_margen := NA]
  dataset[foto_mes == 201910, mactivos_margen := NA]
  dataset[foto_mes == 201910, ccomisiones_otras := NA]
  dataset[foto_mes == 201910, mcomisiones_otras := NA]
  dataset[foto_mes == 201910, mcomisiones := NA]
  dataset[foto_mes == 201910, mrentabilidad := NA]
  dataset[foto_mes == 201910, mrentabilidad_annual := NA]
  dataset[foto_mes == 201910, chomebanking_transacciones := NA]
  dataset[foto_mes == 201910, ctarjeta_visa_descuentos := NA]
  dataset[foto_mes == 201910, ctarjeta_master_descuentos := NA]
  dataset[foto_mes == 201910, mtarjeta_visa_descuentos := NA]
  dataset[foto_mes == 201910, mtarjeta_master_descuentos := NA]
  dataset[foto_mes == 201910, ccajeros_propios_descuentos := NA]
  dataset[foto_mes == 201910, mcajeros_propios_descuentos := NA]
  
  dataset[foto_mes == 202001, cliente_vip := NA]
  
  dataset[foto_mes == 202006, active_quarter := NA]
  dataset[foto_mes == 202006, mrentabilidad := NA]
  dataset[foto_mes == 202006, mrentabilidad_annual := NA]
  dataset[foto_mes == 202006, mcomisiones := NA]
  dataset[foto_mes == 202006, mactivos_margen := NA]
  dataset[foto_mes == 202006, mpasivos_margen := NA]
  dataset[foto_mes == 202006, mcuentas_saldo := NA]
  dataset[foto_mes == 202006, ctarjeta_debito_transacciones := NA]
  dataset[foto_mes == 202006, mautoservicio := NA]
  dataset[foto_mes == 202006, ctarjeta_visa_transacciones := NA]
  dataset[foto_mes == 202006, mtarjeta_visa_consumo := NA]
  dataset[foto_mes == 202006, ctarjeta_master_transacciones := NA]
  dataset[foto_mes == 202006, mtarjeta_master_consumo := NA]
  dataset[foto_mes == 202006, ccomisiones_otras := NA]
  dataset[foto_mes == 202006, mcomisiones_otras := NA]
  dataset[foto_mes == 202006, cextraccion_autoservicio := NA]
  dataset[foto_mes == 202006, mextraccion_autoservicio := NA]
  dataset[foto_mes == 202006, ccheques_depositados := NA]
  dataset[foto_mes == 202006, mcheques_depositados := NA]
  dataset[foto_mes == 202006, ccheques_emitidos := NA]
  dataset[foto_mes == 202006, mcheques_emitidos := NA]
  dataset[foto_mes == 202006, ccheques_depositados_rechazados := NA]
  dataset[foto_mes == 202006, mcheques_depositados_rechazados := NA]
  dataset[foto_mes == 202006, ccheques_emitidos_rechazados := NA]
  dataset[foto_mes == 202006, mcheques_emitidos_rechazados := NA]
  dataset[foto_mes == 202006, tcallcenter := NA]
  dataset[foto_mes == 202006, ccallcenter_transacciones := NA]
  dataset[foto_mes == 202006, thomebanking := NA]
  dataset[foto_mes == 202006, chomebanking_transacciones := NA]
  dataset[foto_mes == 202006, ccajas_transacciones := NA]
  dataset[foto_mes == 202006, ccajas_consultas := NA]
  dataset[foto_mes == 202006, ccajas_depositos := NA]
  dataset[foto_mes == 202006, ccajas_extracciones := NA]
  dataset[foto_mes == 202006, ccajas_otras := NA]
  dataset[foto_mes == 202006, catm_trx := NA]
  dataset[foto_mes == 202006, matm := NA]
  dataset[foto_mes == 202006, catm_trx_other := NA]
  dataset[foto_mes == 202006, matm_other := NA]
  dataset[foto_mes == 202006, ctrx_quarter := NA]
  dataset[foto_mes == 202006, cmobile_app_trx := NA]
  
  cat( "fin Corregir_MachineLearning()\n")
}
#------------------------------------
# Metodo de imputacion MICE

Corregir_MICE <- function(dataset) {
  cat( "inicio MICE()\n")
  
  #Selecciono las variables que voy a imputar, que son una combinacion de las importantes y las que tienen NAs
  to_impute_variables <- c(
    "ctrx_quarter",
    "mpasivos_margen",
    "mrentabilidad_annual",
    "mactivos_margen",
    "mtransferencias_recibidas",
    "mtarjeta_visa_consumo",
    "Visa_mfinanciacion_limite",
    "chomebanking_transacciones",
    "mrentabilidad"
  )
  #methods <- make.method(dataset)
  #methods[to_impute_variables] <- "pmm"
  #methods[setdiff(names(dataset), to_impute_variables)] <- ""
  imputed_data <- mice(dataset[, ..to_impute_variables], m = 5, method = 'pmm')
  # #selecciono las variables imputadas
  # data_impute <- dataset[, ..to_impute_columns]
  # 
  # num_cores <- detectCores() - 1
  # 
  # if (!file.exists("mice.RDATA")) {
  #   # Genero matriz de prediccion
  #   predictor_matrix <- quickpred(dataset, mincor = 0.3, include = names(data_impute), exclude = c("tmobile_app"))
  #   # Elimino las predicciones entre variables imputadas
  #   #predictor_matrix[, to_impute_columns] <- 0
  #   
  #   methods <- make.method(dataset)
  #   methods[to_impute_columns] <- "rf"
  #   methods[setdiff(names(dataset), to_impute_columns)] <- ""
  #   
  #   
  #   imputed_data <- mice(dataset, m = 3, method = methods, predictorMatrix = predictor_matrix, 
  #                      parallel = "multicore",maxit=1, n.core = num_cores )
  #   iter = 1
  #   save(imputed_data,iter,file="mice.RDATA")
  #   completed_data <- complete(imputed_data)
  #   print(paste0("saved iter",str(iter)))
  #   cat( "grabado del dataset\n")
  #   cat( "Iniciando grabado del dataset\n" )
  #   fwrite(completed_data,
  #          file = paste0("impute_cols",str(iter),".csv.gz"),
  #          logical01 = TRUE,
  #          sep = ","
  #   )
  #   cat( "Finalizado grabado del dataset\n" )
  #   
  # } else {
  #   load("mice.RDATA")
  #   print("Checkpoint encontrado, reanudando la imputación.")
  # }
  # while(iter< 6){
  #   imputed_data = mice.mids(imputed_data, parallel = "multicore",maxit=1, n.core = num_cores )
  #   iter = iter + 1
  #   save(imputed_data,iter,file="mice.RDATA")
  completed_data <- complete(imputed_data)
  #   print(paste0("saved iter",str(iter)))
  #   cat( "grabado del dataset\n")
  #   cat( "Iniciando grabado del dataset\n" )
  #   fwrite(completed_data,
  #          file = paste0("impute_cols",str(iter),".csv.gz"),
  #          logical01 = TRUE,
  #          sep = ","
  #   )
  #   cat( "Finalizado grabado del dataset\n" )
  #}
  
  
  # Reemplazar las columnas imputadas en el data.table original
  dataset[, (to_impute_variables) := completed_data[, to_impute_variables]]
  
  
  cat( "fin mice\n")
}


#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Aqui empieza el programa
cat( "z521_CA_reparar_dataset.r  START\n")
action_inicializar() 

# cargo el dataset
envg$PARAM$dataset <- paste0( "./", envg$PARAM$input, "/dataset.csv.gz" )
envg$PARAM$dataset_metadata <- read_yaml( paste0( "./", envg$PARAM$input, "/dataset_metadata.yml" ) )

cat( "lectura del dataset\n")
action_verificar_archivo( envg$PARAM$dataset )
cat( "Iniciando lectura del dataset\n" )
dataset <- fread(envg$PARAM$dataset)
cat( "Finalizada lectura del dataset\n" )

# tmobile_app se daño a partir de 202010

if ("tmobile_app" %in% names(dataset)){
  dataset[, tmobile_app := NULL]
}



GrabarOutput()

# ordeno dataset
setorderv(dataset, envg$PARAM$dataset_metadata$primarykey)

# corrijo los  < foto_mes, campo >  que fueron pisados con cero
switch( envg$PARAM$metodo,
        "MachineLearning"     = Corregir_MachineLearning(dataset),
        "EstadisticaClasica"  = Corregir_EstadisticaClasica(dataset),
        "Ninguno"             = cat("No se aplica ninguna correccion.\n"),
        "MICE"                = Corregir_MICE(dataset)
)


#------------------------------------------------------------------------------
# grabo el dataset
cat( "grabado del dataset\n")
cat( "Iniciando grabado del dataset\n" )
fwrite(dataset,
       file = "dataset.csv.gz",
       logical01 = TRUE,
       sep = ","
)
cat( "Finalizado grabado del dataset\n" )

# copia la metadata sin modificar
cat( "grabado metadata\n")
write_yaml( envg$PARAM$dataset_metadata, 
            file="dataset_metadata.yml" )

#------------------------------------------------------------------------------

# guardo los campos que tiene el dataset
tb_campos <- as.data.table(list(
  "pos" = 1:ncol(dataset),
  "campo" = names(sapply(dataset, class)),
  "tipo" = sapply(dataset, class),
  "nulos" = sapply(dataset, function(x) {
    sum(is.na(x))
  }),
  "ceros" = sapply(dataset, function(x) {
    sum(x == 0, na.rm = TRUE)
  })
))

fwrite(tb_campos,
       file = "dataset.campos.txt",
       sep = "\t"
)

#------------------------------------------------------------------------------
cat( "Fin del programa\n")

envg$OUTPUT$dataset$ncol <- ncol(dataset)
envg$OUTPUT$dataset$nrow <- nrow(dataset)
envg$OUTPUT$time$end <- format(Sys.time(), "%Y%m%d %H%M%S")
GrabarOutput()

#------------------------------------------------------------------------------
# finalizo la corrida
#  archivos tiene a los files que debo verificar existen para no abortar

action_finalizar( archivos = c("dataset.csv.gz","dataset_metadata.yml")) 
cat( "z521_CA_reparar_dataset.r  END\n")