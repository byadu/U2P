library(dplyr)
library(libcubmeta)
library(libcubolap)
library(RMySQL)
library(libcubolap)

getsel<- function(cfg, rep) {
	gf<- data.frame(tbl(cfg, 'graph_def') %>% filter(gf_id==rep) %>% select(gf_id, gf_uid, gf_user_title, gf_dimn, gf_folder))
	gs<- data.frame(tbl(cfg, 'graph_series') %>% filter(gs_id==!!gf[1,1]) %>% select(gs_series_id))[,1]
	ds<- data.frame(tbl(cfg, 'data_series') %>% filter(ds_series_id %in% !!gs) %>% select(ds_yid, ds_xids, ds_filt_xids))
	yid<- ds$ds_yid
	xids<- unique(ds$ds_xids)
	filtids<- unique(ds$ds_filt_xids)
	dv<- data.frame(tbl(cfg, 'data_seriesvals') %>% filter(dv_seriesid %in% gs) %>% select(dv_seriesid, dv_xid, dv_xidval, dv_seq)) %>% arrange(dv_seriesid, dv_xid, dv_seq)
	return(list(name=gf$gf_user_title, folder=gf$gf_folder, uid=gf$gf_uid, dimn=gf$gf_dimn, yid=yid, xids=xids, filtids=filtids, dv=dv))
	}

fetchcache<- function(cfg, sel) {
	yid<- sel$yid[1]
	xids<- sel$xids
	filtids<- sel$filtids
	dv<- sel$dv
	
	tds<- tbl(cfg, 'data_series')
	dsids<- filter(tds, ds_xids==xids & ds_yid == yid & ds_filt_xids==filtids) %>% select(ds_series_id)
	dsids<- data.frame(dsids)[,1]
		
	if(length(dsids) == 0) return(0)

	xids<- strsplit(filtids,',')[[1]]

	for(k in 1:length(dsids)) {
		matched<- 1
		tdv<- tbl(cfg, 'data_seriesvals') %>% filter(dv_seriesid == !!dsids[k]) 
		
		for(j in 1:length(xids)) {
			selxidval<- filter(dv, dv_xid == !!xids[j]) %>% select(dv_xidval)
			selxidval<- selxidval[,1]
			cachexidval<- data.frame(tdv %>% filter(dv_xid==!!xids[j]) %>% select(dv_xid, dv_xidval, dv_seq) %>% arrange(dv_seq))
			for (i in 1:length(selxidval))
				if(selxidval[i] != cachexidval[i,2]) {
					matched<- 0
					break
					}
			if(!matched) break
			}
		if(matched) return(dsids[k])
		}
	return(0)
	}

add_ds<- function(mycfg, sel) {
	newds<- next_series_seq(mycfg)
	ds<- cbind(newds[1], sel$yid, sel$xids, sel$filtids)
	ds<- data.frame(ds)
	colnames(ds)<- c('ds_series_id', 'ds_yid', 'ds_xids', 'ds_filt_xids')
	dbWriteTable(mycfg, 'data_series', ds, append=T, row.names=F)
	if(nrow(sel$dv) > 0) {
		dv<- sel$dv
		dv$dv_seriesid<- newds
		dbWriteTable(mycfg, 'data_seriesvals', dv, append=T, row.names=F)
		}
	return(newds)
	}

add_gf<- function(mycfg, sel, newds) {
	newgf<- next_graph_seq(mycfg)

	gs<- as.data.frame(cbind(newgf, newds, c(1:length(newds))))
	colnames(gs)<- c('gs_id', 'gs_series_id', 'gs_series_num')
	dbWriteTable(mycfg, "graph_series", gs, row.names=F, append=T)

	gf<- as.data.frame(cbind(sel$uid, newgf, sel$name, sel$dimn, sel$folder))
	colnames(gf)<- c('gf_uid', 'gf_id', 'gf_user_title', 'gf_dimn', 'gf_folder')
	dbWriteTable(mycfg, "graph_def", gf, row.names=F, append=T)

	return(newgf)
	}

putrep<- function(cfgp, mycfgp, sel) {
	yid<- sel$yid
	dv<- sel$dv
	dvs<- sel$dv$dv_seriesid
	sel1<- sel
	added_ds<- 0
	newds<- c()
	for(i in 1:length(yid)) {
		sel1$yid<- sel$yid[i]
		if(length(dvs) > 0) {
			dv1<- filter(dv, dv$dv_seriesid == !!dvs[i])
			sel1$dv<- dv1
			}
		ds<- fetchcache(cfgp, sel1)
		if(!ds) {
			ds<- add_ds(mycfgp, sel1)
			added_ds<- 1
			}
		newds<- c(newds, ds)
		}

	if(added_ds) {
		newgf<- add_gf(mycfgp, sel, newds)
		}
	else {
		gfp<- tbl(cfgp, 'graph_def') %>% filter(gf_user_title==!!sel$name & gf_uid == !!sel$uid)
		gfp<- data.frame(gfp)
		if(nrow(gfp)==0)
			newgf<- add_gf(mycfgp, sel, newds)
		else
			newgf<- 0 
		}
	}

rep<- commandArgs(trailingOnly=T)

cfg<- opendb('finst')

cfgp<- opendb('finst2')
mycfgp<-dbConnect(MySQL(), user='cubot',password='',dbname='finst2', host='localhost')

for(i in 1:length(rep)) {
	sel<- getsel(cfg, rep[i])
	print(putrep(cfgp, mycfgp, sel))
	}
