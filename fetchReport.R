# AUTHOR: OMKAR SUNKERSETT, UNIVERSITY OF MICHIGAN, ANN ARBOR
# PURPOSE: TO FETCH ENERGY MARKET-PRICE DATA FROM THE DATABASE AND PREPARE A REPORT IN CSV FORMAT

library(RMySQL)

fetchReport <- function(market_name, start_dt, end_dt) {
  mydb <- dbConnect(MySQL(), user='not-published', password='not-published', dbname='not-published', host='not-published', port=not-published)
  
  if(market_name == 'CAISO') {
    caiso_qry = paste("SELECT w.market_id, w.market_name, x.offer_id, x.identifier_1, x.identifier_2, x.region_name, y.interval_id, y.mkt_run_id, y.interval_dt, y.interval_start, y.interval_end, y.opr_hour, y.opr_interval, z.nsreq_max, z.nsreq_min, z.rdreq_max, z.rdreq_min, z.rmdreq_max, z.rmdreq_min, z.rureq_max, z.rureq_min, z.rmureq_max, z.rmureq_min, z.spreq_max, z.spreq_min, z.nsproc_cap, z.nsself_cap, z.nscost_line, z.nsclr_price, z.nstot_cap, z.rdproc_cap, z.rdself_cap, z.rdcost_line, z.rdclr_price, z.rdtot_cap, z.rmdproc_cap, z.rmdself_cap, z.rmdcost_line, z.rmdclr_price, z.rmdtot_cap, z.ruproc_cap, z.ruself_cap, z.rucost_line, z.ruclr_price, z.rutot_cap, z.rmuproc_cap, z.rmuself_cap, z.rmucost_line, z.rmuclr_price, z.rmutot_cap, z.spproc_cap, z.spself_cap, z.spcost_line, z.spclr_price, z.sptot_cap FROM market_meta w USE INDEX (primary) INNER JOIN (offer_base x USE INDEX (primary, idx_offer_base_market_id) INNER JOIN (interval_meta y USE INDEX (primary, idx_interval_meta_market_id) INNER JOIN caiso_results z USE INDEX (idx_caiso_results_interval_id) ON y.interval_id = z.interval_id) ON x.offer_id = y.offer_id) ON w.market_id = x.market_id WHERE lower(market_name) = 'caiso' AND y.interval_dt >= '",start_dt,"' AND y.interval_dt <= '",end_dt,"';", sep = "")
    rs <- dbSendQuery(mydb, caiso_qry)
  }
  else if(market_name == 'MISO') {
    miso_qry = paste("SELECT w.market_id, w.market_name, x.offer_id, x.identifier_1, x.identifier_2, x.region_name, y.interval_id, y.mkt_run_id, y.interval_dt, y.interval_start, y.interval_end, y.opr_hour, y.opr_interval, z.reg_max, z.reg_min, z.regoff_price, z.regself_limit, z.spinoff_price, z.spinself_limit, z.onsupp_price, z.onsuppself_limit, z.offsupp_price, z.offsuppself_limit, z.regavg_mcp, z.regavg_cap, z.spinavg_mcp, z.spinavg_cap, z.suppavg_mcp, z.suppavg_cap FROM market_meta w USE INDEX (primary) INNER JOIN (offer_base x USE INDEX (primary, idx_offer_base_market_id) INNER JOIN (interval_meta y USE INDEX (primary, idx_interval_meta_market_id) INNER JOIN miso_results z USE INDEX (idx_miso_results_interval_id) ON y.interval_id = z.interval_id) ON x.offer_id = y.offer_id) ON w.market_id = x.market_id WHERE lower(market_name) = 'miso' AND y.interval_dt >= '",start_dt,"' AND y.interval_dt <= '",end_dt,"';", sep = "")
    rs <- dbSendQuery(mydb, miso_qry)
  }
  else if(market_name == 'ISO-NE') {
    isone_qry = paste("SELECT w.market_id, w.market_name, x.offer_id, x.identifier_1, x.identifier_2, x.region_name, y.interval_id, y.mkt_run_id, y.interval_dt, y.interval_start, y.interval_end, y.opr_hour, y.opr_interval, z.reglimit_low, z.reglimit_high, z.reg_status, z.autoresp_rate, z.regoff_price, z.regserv_price, z.regcap_price, z.regito_cost FROM market_meta w USE INDEX (primary) INNER JOIN (offer_base x USE INDEX (primary, idx_offer_base_market_id) INNER JOIN (interval_meta y USE INDEX (primary, idx_interval_meta_market_id) INNER JOIN isone_results z USE INDEX (idx_isone_results_interval_id) ON y.interval_id = z.interval_id) ON x.offer_id = y.offer_id) ON w.market_id = x.market_id WHERE lower(market_name) = 'iso-ne' AND y.interval_dt >= '",start_dt,"' AND y.interval_dt <= '",end_dt,"';", sep = "")
    rs <- dbSendQuery(mydb, isone_qry)
  }
  else if(market_name == 'SPP') {
    spp_qry = paste("SELECT w.market_id, w.market_name, x.offer_id, x.identifier_1, x.identifier_2, x.region_name, y.interval_id, y.mkt_run_id, y.interval_dt, y.interval_start, y.interval_end, y.opr_hour, y.opr_interval, z.coreg_down, z.coreg_up, z.mfreg_down, z.mfreg_up, z.moreg_down, z.moreg_up, z.spin_price, z.supp_price FROM market_meta w USE INDEX (primary) INNER JOIN (offer_base x USE INDEX (primary, idx_offer_base_market_id) INNER JOIN (interval_meta y USE INDEX (primary, idx_interval_meta_market_id) INNER JOIN spp_results z USE INDEX (idx_spp_results_interval_id) ON y.interval_id = z.interval_id) ON x.offer_id = y.offer_id) ON w.market_id = x.market_id WHERE lower(market_name) = 'spp' AND y.interval_dt >= '",start_dt,"' AND y.interval_dt <= '",end_dt,"';", sep = "")
    rs <- dbSendQuery(mydb, spp_qry)
  }
  
  df <- fetch(rs, n = -1)
  write.csv(df, paste('/Users/omkarsunkersett/Downloads/RPT_',market_name,'_',start_dt,'_',end_dt,'.csv', sep = ""), sep = ",", row.names = FALSE, col.names = TRUE)
  
  dbClearResult(rs)
  dbDisconnect(mydb)
}

#fetchReport('CAISO', '2017-01-01', '2017-01-31')
