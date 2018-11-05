package com.deloitte.missiongraph.domain

import com.datastax.driver.core.DataType
import com.datastax.driver.core.schemabuilder.SchemaBuilder
import com.deloitte.missiongraph.GraphSchema._
import com.deloitte.missiongraph.sourcedata.emilpo
import org.apache.spark.sql.types.{StringType, StructField, StructType}

class soldier extends emilpo {

  var transactionalTable: String = "transactional_soldier"
  def setTransactionalTableName(tableName: String): Unit = this.transactionalTable = tableName
  var entityTable: String = "entity_soldier"
  def setEntityTableName(tableName: String): Unit = this.entityTable = tableName
  var delimiter = "|"
  def setDelimiter(delimiter: String): Unit = this.delimiter = delimiter
  def setFileLocation(fileLocation: String): Unit = this.fileLocation = fileLocation
  def getSchemaColumns: List[(String,String)] = schema.toList.map(x => (x.productElement(0).toString,x.productElement(1).toString))

  def schema:StructType = {
    StructType(Array(
      StructField("rgmt_cd", StringType, nullable = true),
      StructField("conus_in_cd", StringType, nullable = true),
      StructField("mil_svc_comp_cd", StringType, nullable = true),
      StructField("compo_cd", StringType, nullable = true),
      StructField("mil_pers_clas_cd", StringType, nullable = true),
      StructField("uic", StringType, nullable = false),
      StructField("rgmt_homebase_cd", StringType, nullable = true),
      StructField("ad_vol_stat_cd", StringType, nullable = true),
      StructField("ad_avail_ym", StringType, nullable = true),
      StructField("photo_dt", StringType, nullable = true),
      StructField("mil_pay_lvl_ser_nr", StringType, nullable = true),
      StructField("sgli_covg_cd", StringType, nullable = true),
      StructField("ctrl_grp_cd", StringType, nullable = true),
      StructField("deers_enrl_stat_cd", StringType, nullable = true),
      StructField("deers_enrl_stat_dt", StringType, nullable = true),
      StructField("prom_cons_rcmd_cd", StringType, nullable = true),
      StructField("init_mil_entry_dt", StringType, nullable = true),
      StructField("mil_ead_dt", StringType, nullable = true),
      StructField("unit_rlse_dt", StringType, nullable = true),
      StructField("unit_rlse_mo_qy", StringType, nullable = true),
      StructField("order_rstrctn_dt", StringType, nullable = true),
      StructField("act_fed_svc_mo_qy", StringType, nullable = true),
      StructField("act_fed_svc_day_qy", StringType, nullable = true),
      StructField("pri_act_dispt_cd", StringType, nullable = true),
      StructField("pers_act_pend_cd", StringType, nullable = true),
      StructField("rc_tng_ret_grp_cd", StringType, nullable = true),
      StructField("non_mil_emplr_cd", StringType, nullable = true),
      StructField("rc_cat_cd", StringType, nullable = true),
      StructField("arng_ppn_cd", StringType, nullable = true),
      StructField("sel_res_trnstn_cd", StringType, nullable = true),
      StructField("prom_svc_st_dt", StringType, nullable = true),
      StructField("afrm_awd_elig_ym", StringType, nullable = true),
      StructField("arcam_awd_elig_ym", StringType, nullable = true),
      StructField("basd_dt", StringType, nullable = true),
      StructField("besd_dt", StringType, nullable = true),
      StructField("bosd_dt", StringType, nullable = true),
      StructField("pebd_dt", StringType, nullable = true),
      StructField("rc_gain_dt", StringType, nullable = true),
      StructField("dual_mpc_cd", StringType, nullable = true),
      StructField("dual_comp_stat_cd", StringType, nullable = true),
      StructField("dual_comp_oer_dt", StringType, nullable = true),
      StructField("eval_pd_end_dt", StringType, nullable = true),
      StructField("prom_list_seq_nr", StringType, nullable = true),
      StructField("prom_list_dt", StringType, nullable = true),
      StructField("src_call_ad_uic_cd", StringType, nullable = true),
      StructField("off_ppn_cd", StringType, nullable = true),
      StructField("appt_svc_comp_cd", StringType, nullable = true),
      StructField("cmd_stat_cd", StringType, nullable = true),
      StructField("short_os_tour_qy", StringType, nullable = true),
      StructField("long_os_tour_qy", StringType, nullable = true),
      StructField("sep_delay_rsn_cd", StringType, nullable = true),
      StructField("pers_str_stat_cd", StringType, nullable = true),
      StructField("mil_id_card_nr", StringType, nullable = true),
      StructField("mil_id_card_typ", StringType, nullable = true),
      StructField("sys_update_user_id_x", StringType, nullable = true),
      StructField("sys_update_dt_x", StringType, nullable = true),
      StructField("sgli_elect_dt", StringType, nullable = true),
      StructField("sgli_ent_ubc_dt", StringType, nullable = true),
      StructField("pers_data_ver_dt", StringType, nullable = true),
      StructField("mil_ppa_cd", StringType, nullable = true),
      StructField("corr_crs_cr_hr_compl", StringType, nullable = true),
      StructField("corr_crs_ch_com_dt", StringType, nullable = true),
      StructField("asg_pcs_dt", StringType, nullable = true),
      StructField("asg_deros_dt", StringType, nullable = true),
      StructField("asg_dros_dt", StringType, nullable = true),
      StructField("prev_pers_str_stat_cd", StringType, nullable = true),
      StructField("soldier_ssn", StringType, nullable = true),
      StructField("edipi", StringType, nullable = true),
      StructField("drrsa_pers_id2", StringType, nullable = true),
      StructField("fullname", StringType, nullable = true),
      StructField("firstname", StringType, nullable = true),
      StructField("lastname", StringType, nullable = true),
      StructField("middlename", StringType, nullable = true),
      StructField("address_street", StringType, nullable = true),
      StructField("address_city", StringType, nullable = true),
      StructField("address_state", StringType, nullable = true),
      StructField("etsdate", StringType, nullable = true),
      StructField("mos", StringType, nullable = true),
      StructField("securityclearance", StringType, nullable = true),
      StructField("rank", StringType, nullable = true),
      StructField("grade", StringType, nullable = true),
      StructField("birthcountry", StringType, nullable = true),
      StructField("ssn_x", StringType, nullable = true),
      StructField("scty_clr_dt", StringType, nullable = true),
      StructField("scty_clr_lvl_cd", StringType, nullable = true),
      StructField("scty_clr_authty_cd", StringType, nullable = true),
      StructField("sys_update_user_id_y", StringType, nullable = true),
      StructField("sys_update_dt_y", StringType, nullable = true),
      StructField("secclearance", StringType, nullable = true),
      StructField("latest_date", StringType, nullable = true),
      StructField("latest_date_flag", StringType, nullable = true),
      StructField("securityclass", StringType, nullable = true),
      StructField("ssn_y", StringType, nullable = true),
      StructField("birth_city_nm", StringType, nullable = true),
      StructField("birth_cnty_cd", StringType, nullable = true),
      StructField("birth_cntry_cd", StringType, nullable = true),
      StructField("sex_cd", StringType, nullable = true),
      StructField("race_pop_cd", StringType, nullable = true),
      StructField("birth_state_ab", StringType, nullable = true),
      StructField("ethnic_grp_cd", StringType, nullable = true),
      StructField("ctzsp_cntry_cd", StringType, nullable = true),
      StructField("ctzsp_orgn_cd", StringType, nullable = true),
      StructField("martl_stat_dt", StringType, nullable = true),
      StructField("birth_dt", StringType, nullable = true),
      StructField("depn_qy", StringType, nullable = true),
      StructField("martl_stat_cd", StringType, nullable = true),
      StructField("adult_depn_qy", StringType, nullable = true),
      StructField("child_depn_qy", StringType, nullable = true),
      StructField("rel_denom_cd", StringType, nullable = true),
      StructField("prior_mil_svc_cd", StringType, nullable = true),
      StructField("sel_svc_class_cd", StringType, nullable = true),
      StructField("ssn_ver_cd", StringType, nullable = true),
      StructField("prev_ssn", StringType, nullable = true),
      StructField("blood_typ_grp_cd", StringType, nullable = true),
      StructField("blood_typ_ant_cd", StringType, nullable = true),
      StructField("email_addr_tx", StringType, nullable = true),
      StructField("alt_email_addr_tx", StringType, nullable = true),
      StructField("total_army_comp_cd", StringType, nullable = true),
      StructField("total_army_cat_cd", StringType, nullable = true),
      StructField("tapdb_rec_stat_cd", StringType, nullable = true),
      StructField("tapdb_rec_stat_dt", StringType, nullable = true),
      StructField("civ_ed_lvl_cd", StringType, nullable = true),
      StructField("birth_pl_val_dt", StringType, nullable = true),
      StructField("ctzsp_val_dt", StringType, nullable = true),
      StructField("unit_lname", StringType, nullable = true)
    ))
  }


  def dseSchemaTransactional(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, transactionalTable)
      .ifNotExists()
      .addPartitionKey("UIC", DataType.text())
      .addClusteringColumn("INSERT_TIME", DataType.timestamp())
      .addColumn("RGMT_CD", DataType.text())
      .addColumn("CONUS_IN_CD", DataType.text())
      .addColumn("MIL_SVC_COMP_CD", DataType.text())
      .addColumn("COMPO_CD", DataType.text())
      .addColumn("MIL_PERS_CLAS_CD", DataType.text())
      .addColumn("RGMT_HOMEBASE_CD", DataType.text())
      .addColumn("AD_VOL_STAT_CD", DataType.text())
      .addColumn("AD_AVAIL_YM", DataType.text())
      .addColumn("PHOTO_DT", DataType.text())
      .addColumn("MIL_PAY_LVL_SER_NR", DataType.text())
      .addColumn("SGLI_COVG_CD", DataType.text())
      .addColumn("CTRL_GRP_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_DT", DataType.text())
      .addColumn("PROM_CONS_RCMD_CD", DataType.text())
      .addColumn("INIT_MIL_ENTRY_DT", DataType.text())
      .addColumn("MIL_EAD_DT", DataType.text())
      .addColumn("UNIT_RLSE_DT", DataType.text())
      .addColumn("UNIT_RLSE_MO_QY", DataType.text())
      .addColumn("ORDER_RSTRCTN_DT", DataType.text())
      .addColumn("ACT_FED_SVC_MO_QY", DataType.text())
      .addColumn("ACT_FED_SVC_DAY_QY", DataType.text())
      .addColumn("PRI_ACT_DISPT_CD", DataType.text())
      .addColumn("PERS_ACT_PEND_CD", DataType.text())
      .addColumn("RC_TNG_RET_GRP_CD", DataType.text())
      .addColumn("NON_MIL_EMPLR_CD", DataType.text())
      .addColumn("RC_CAT_CD", DataType.text())
      .addColumn("ARNG_PPN_CD", DataType.text())
      .addColumn("SEL_RES_TRNSTN_CD", DataType.text())
      .addColumn("PROM_SVC_ST_DT", DataType.text())
      .addColumn("AFRM_AWD_ELIG_YM", DataType.text())
      .addColumn("ARCAM_AWD_ELIG_YM", DataType.text())
      .addColumn("BASD_DT", DataType.text())
      .addColumn("BESD_DT", DataType.text())
      .addColumn("BOSD_DT", DataType.text())
      .addColumn("PEBD_DT", DataType.text())
      .addColumn("RC_GAIN_DT", DataType.text())
      .addColumn("DUAL_MPC_CD", DataType.text())
      .addColumn("DUAL_COMP_STAT_CD", DataType.text())
      .addColumn("DUAL_COMP_OER_DT", DataType.text())
      .addColumn("EVAL_PD_END_DT", DataType.text())
      .addColumn("PROM_LIST_SEQ_NR", DataType.text())
      .addColumn("PROM_LIST_DT", DataType.text())
      .addColumn("SRC_CALL_AD_UIC_CD", DataType.text())
      .addColumn("OFF_PPN_CD", DataType.text())
      .addColumn("APPT_SVC_COMP_CD", DataType.text())
      .addColumn("CMD_STAT_CD", DataType.text())
      .addColumn("SHORT_OS_TOUR_QY", DataType.text())
      .addColumn("LONG_OS_TOUR_QY", DataType.text())
      .addColumn("SEP_DELAY_RSN_CD", DataType.text())
      .addColumn("PERS_STR_STAT_CD", DataType.text())
      .addColumn("MIL_ID_CARD_NR", DataType.text())
      .addColumn("MIL_ID_CARD_TYP", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_x", DataType.text())
      .addColumn("SYS_UPDATE_DT_x", DataType.text())
      .addColumn("SGLI_ELECT_DT", DataType.text())
      .addColumn("SGLI_ENT_UBC_DT", DataType.text())
      .addColumn("PERS_DATA_VER_DT", DataType.text())
      .addColumn("MIL_PPA_CD", DataType.text())
      .addColumn("CORR_CRS_CR_HR_COMPL", DataType.text())
      .addColumn("CORR_CRS_CH_COM_DT", DataType.text())
      .addColumn("ASG_PCS_DT", DataType.text())
      .addColumn("ASG_DEROS_DT", DataType.text())
      .addColumn("ASG_DROS_DT", DataType.text())
      .addColumn("PREV_PERS_STR_STAT_CD", DataType.text())
      .addColumn("Soldier_SSN", DataType.text())
      .addColumn("EDIPI", DataType.text())
      .addColumn("DRRSA_PERS_ID2", DataType.text())
      .addColumn("FullName", DataType.text())
      .addColumn("FirstName", DataType.text())
      .addColumn("LastName", DataType.text())
      .addColumn("MiddleName", DataType.text())
      .addColumn("Address_Street", DataType.text())
      .addColumn("Address_City", DataType.text())
      .addColumn("Address_State", DataType.text())
      .addColumn("ETSDate", DataType.text())
      .addColumn("MOS", DataType.text())
      .addColumn("SecurityClearance", DataType.text())
      .addColumn("Rank", DataType.text())
      .addColumn("Grade", DataType.text())
      .addColumn("BirthCountry", DataType.text())
      .addColumn("SSN_x", DataType.text())
      .addColumn("SCTY_CLR_DT", DataType.text())
      .addColumn("SCTY_CLR_LVL_CD", DataType.text())
      .addColumn("SCTY_CLR_AUTHTY_CD", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_y", DataType.text())
      .addColumn("SYS_UPDATE_DT_y", DataType.text())
      .addColumn("SecCLearance", DataType.text())
      .addColumn("Latest_Date", DataType.text())
      .addColumn("Latest_Date_Flag", DataType.text())
      .addColumn("SecurityClass", DataType.text())
      .addColumn("SSN_y", DataType.text())
      .addColumn("BIRTH_CITY_NM", DataType.text())
      .addColumn("BIRTH_CNTY_CD", DataType.text())
      .addColumn("BIRTH_CNTRY_CD", DataType.text())
      .addColumn("SEX_CD", DataType.text())
      .addColumn("RACE_POP_CD", DataType.text())
      .addColumn("BIRTH_STATE_AB", DataType.text())
      .addColumn("ETHNIC_GRP_CD", DataType.text())
      .addColumn("CTZSP_CNTRY_CD", DataType.text())
      .addColumn("CTZSP_ORGN_CD", DataType.text())
      .addColumn("MARTL_STAT_DT", DataType.text())
      .addColumn("BIRTH_DT", DataType.text())
      .addColumn("DEPN_QY", DataType.text())
      .addColumn("MARTL_STAT_CD", DataType.text())
      .addColumn("ADULT_DEPN_QY", DataType.text())
      .addColumn("CHILD_DEPN_QY", DataType.text())
      .addColumn("REL_DENOM_CD", DataType.text())
      .addColumn("PRIOR_MIL_SVC_CD", DataType.text())
      .addColumn("SEL_SVC_CLASS_CD", DataType.text())
      .addColumn("SSN_VER_CD", DataType.text())
      .addColumn("PREV_SSN", DataType.text())
      .addColumn("BLOOD_TYP_GRP_CD", DataType.text())
      .addColumn("BLOOD_TYP_ANT_CD", DataType.text())
      .addColumn("EMAIL_ADDR_TX", DataType.text())
      .addColumn("ALT_EMAIL_ADDR_TX", DataType.text())
      .addColumn("TOTAL_ARMY_COMP_CD", DataType.text())
      .addColumn("TOTAL_ARMY_CAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_DT", DataType.text())
      .addColumn("CIV_ED_LVL_CD", DataType.text())
      .addColumn("BIRTH_PL_VAL_DT", DataType.text())
      .addColumn("CTZSP_VAL_DT", DataType.text())
      .addColumn("UNIT_LNAME", DataType.text())
      .addColumn("processed", DataType.cboolean())
      .addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }

  def dseSchemaEntity(keyspace: String) = {
    SchemaBuilder
      .createTable(keyspace, entityTable)
      .ifNotExists()
      .addPartitionKey("UIC", DataType.text())
      .addColumn("RGMT_CD", DataType.text())
      .addColumn("CONUS_IN_CD", DataType.text())
      .addColumn("MIL_SVC_COMP_CD", DataType.text())
      .addColumn("COMPO_CD", DataType.text())
      .addColumn("MIL_PERS_CLAS_CD", DataType.text())
      .addColumn("RGMT_HOMEBASE_CD", DataType.text())
      .addColumn("AD_VOL_STAT_CD", DataType.text())
      .addColumn("AD_AVAIL_YM", DataType.text())
      .addColumn("PHOTO_DT", DataType.text())
      .addColumn("MIL_PAY_LVL_SER_NR", DataType.text())
      .addColumn("SGLI_COVG_CD", DataType.text())
      .addColumn("CTRL_GRP_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_CD", DataType.text())
      .addColumn("DEERS_ENRL_STAT_DT", DataType.text())
      .addColumn("PROM_CONS_RCMD_CD", DataType.text())
      .addColumn("INIT_MIL_ENTRY_DT", DataType.text())
      .addColumn("MIL_EAD_DT", DataType.text())
      .addColumn("UNIT_RLSE_DT", DataType.text())
      .addColumn("UNIT_RLSE_MO_QY", DataType.text())
      .addColumn("ORDER_RSTRCTN_DT", DataType.text())
      .addColumn("ACT_FED_SVC_MO_QY", DataType.text())
      .addColumn("ACT_FED_SVC_DAY_QY", DataType.text())
      .addColumn("PRI_ACT_DISPT_CD", DataType.text())
      .addColumn("PERS_ACT_PEND_CD", DataType.text())
      .addColumn("RC_TNG_RET_GRP_CD", DataType.text())
      .addColumn("NON_MIL_EMPLR_CD", DataType.text())
      .addColumn("RC_CAT_CD", DataType.text())
      .addColumn("ARNG_PPN_CD", DataType.text())
      .addColumn("SEL_RES_TRNSTN_CD", DataType.text())
      .addColumn("PROM_SVC_ST_DT", DataType.text())
      .addColumn("AFRM_AWD_ELIG_YM", DataType.text())
      .addColumn("ARCAM_AWD_ELIG_YM", DataType.text())
      .addColumn("BASD_DT", DataType.text())
      .addColumn("BESD_DT", DataType.text())
      .addColumn("BOSD_DT", DataType.text())
      .addColumn("PEBD_DT", DataType.text())
      .addColumn("RC_GAIN_DT", DataType.text())
      .addColumn("DUAL_MPC_CD", DataType.text())
      .addColumn("DUAL_COMP_STAT_CD", DataType.text())
      .addColumn("DUAL_COMP_OER_DT", DataType.text())
      .addColumn("EVAL_PD_END_DT", DataType.text())
      .addColumn("PROM_LIST_SEQ_NR", DataType.text())
      .addColumn("PROM_LIST_DT", DataType.text())
      .addColumn("SRC_CALL_AD_UIC_CD", DataType.text())
      .addColumn("OFF_PPN_CD", DataType.text())
      .addColumn("APPT_SVC_COMP_CD", DataType.text())
      .addColumn("CMD_STAT_CD", DataType.text())
      .addColumn("SHORT_OS_TOUR_QY", DataType.text())
      .addColumn("LONG_OS_TOUR_QY", DataType.text())
      .addColumn("SEP_DELAY_RSN_CD", DataType.text())
      .addColumn("PERS_STR_STAT_CD", DataType.text())
      .addColumn("MIL_ID_CARD_NR", DataType.text())
      .addColumn("MIL_ID_CARD_TYP", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_x", DataType.text())
      .addColumn("SYS_UPDATE_DT_x", DataType.text())
      .addColumn("SGLI_ELECT_DT", DataType.text())
      .addColumn("SGLI_ENT_UBC_DT", DataType.text())
      .addColumn("PERS_DATA_VER_DT", DataType.text())
      .addColumn("MIL_PPA_CD", DataType.text())
      .addColumn("CORR_CRS_CR_HR_COMPL", DataType.text())
      .addColumn("CORR_CRS_CH_COM_DT", DataType.text())
      .addColumn("ASG_PCS_DT", DataType.text())
      .addColumn("ASG_DEROS_DT", DataType.text())
      .addColumn("ASG_DROS_DT", DataType.text())
      .addColumn("PREV_PERS_STR_STAT_CD", DataType.text())
      .addColumn("Soldier_SSN", DataType.text())
      .addColumn("EDIPI", DataType.text())
      .addColumn("DRRSA_PERS_ID2", DataType.text())
      .addColumn("FullName", DataType.text())
      .addColumn("FirstName", DataType.text())
      .addColumn("LastName", DataType.text())
      .addColumn("MiddleName", DataType.text())
      .addColumn("Address_Street", DataType.text())
      .addColumn("Address_City", DataType.text())
      .addColumn("Address_State", DataType.text())
      .addColumn("ETSDate", DataType.text())
      .addColumn("MOS", DataType.text())
      .addColumn("SecurityClearance", DataType.text())
      .addColumn("Rank", DataType.text())
      .addColumn("Grade", DataType.text())
      .addColumn("BirthCountry", DataType.text())
      .addColumn("SSN_x", DataType.text())
      .addColumn("SCTY_CLR_DT", DataType.text())
      .addColumn("SCTY_CLR_LVL_CD", DataType.text())
      .addColumn("SCTY_CLR_AUTHTY_CD", DataType.text())
      .addColumn("SYS_UPDATE_USER_ID_y", DataType.text())
      .addColumn("SYS_UPDATE_DT_y", DataType.text())
      .addColumn("SecCLearance", DataType.text())
      .addColumn("Latest_Date", DataType.text())
      .addColumn("Latest_Date_Flag", DataType.text())
      .addColumn("SecurityClass", DataType.text())
      .addColumn("SSN_y", DataType.text())
      .addColumn("BIRTH_CITY_NM", DataType.text())
      .addColumn("BIRTH_CNTY_CD", DataType.text())
      .addColumn("BIRTH_CNTRY_CD", DataType.text())
      .addColumn("SEX_CD", DataType.text())
      .addColumn("RACE_POP_CD", DataType.text())
      .addColumn("BIRTH_STATE_AB", DataType.text())
      .addColumn("ETHNIC_GRP_CD", DataType.text())
      .addColumn("CTZSP_CNTRY_CD", DataType.text())
      .addColumn("CTZSP_ORGN_CD", DataType.text())
      .addColumn("MARTL_STAT_DT", DataType.text())
      .addColumn("BIRTH_DT", DataType.text())
      .addColumn("DEPN_QY", DataType.text())
      .addColumn("MARTL_STAT_CD", DataType.text())
      .addColumn("ADULT_DEPN_QY", DataType.text())
      .addColumn("CHILD_DEPN_QY", DataType.text())
      .addColumn("REL_DENOM_CD", DataType.text())
      .addColumn("PRIOR_MIL_SVC_CD", DataType.text())
      .addColumn("SEL_SVC_CLASS_CD", DataType.text())
      .addColumn("SSN_VER_CD", DataType.text())
      .addColumn("PREV_SSN", DataType.text())
      .addColumn("BLOOD_TYP_GRP_CD", DataType.text())
      .addColumn("BLOOD_TYP_ANT_CD", DataType.text())
      .addColumn("EMAIL_ADDR_TX", DataType.text())
      .addColumn("ALT_EMAIL_ADDR_TX", DataType.text())
      .addColumn("TOTAL_ARMY_COMP_CD", DataType.text())
      .addColumn("TOTAL_ARMY_CAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_CD", DataType.text())
      .addColumn("TAPDB_REC_STAT_DT", DataType.text())
      .addColumn("CIV_ED_LVL_CD", DataType.text())
      .addColumn("BIRTH_PL_VAL_DT", DataType.text())
      .addColumn("CTZSP_VAL_DT", DataType.text())
      .addColumn("UNIT_LNAME", DataType.text())
      //.addColumn("processed", DataType.cboolean())
      //.addColumn("marked_for_delete", DataType.cboolean())
      .withOptions()
      .compactionOptions(SchemaBuilder.timeWindowCompactionStrategy())
  }

  private val partitionProperties = List(
    property("UIC",GraphDataType.Text,Cardinality.single)
  )

  private val clusteringProperties = List(
    property("INSERT_TIME",GraphDataType.Text,Cardinality.single)
  )

  private val properties = List(
    property("UNIT_LNAME",GraphDataType.Text,Cardinality.single),
    property("UNIT_HOME_FORT_NAME",GraphDataType.Text,Cardinality.single),
    property("UNIT_LOCATION_ZIP_CODE",GraphDataType.Text,Cardinality.single),
    property("UNIT_GEOCODE",GraphDataType.Text,Cardinality.single),
    property("EquipUIC",GraphDataType.Text,Cardinality.single)
  )

  private val allProperties = partitionProperties ::: clusteringProperties ::: properties

  private val unitVertex = vertex("soldier", properties, partitionProperties, clusteringProperties)

  def getGraphSchema: Map[String, List[String]] = Map(
    GraphObject.Vertex.toString -> List(createVertexSchema(unitVertex)),
    GraphObject.Property.toString -> createPropertySchema(allProperties)
  )

}













