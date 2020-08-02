import os
import datetime


def write_to_file(file_path, write_str):
    file_writer = open(file_path, 'ab+')
    file_writer.write(write_str.encode("utf-8"))
    file_writer.close()


seq_str1 = '''
<JOBS_UNIX AttrType="UNIX" name="SH_O_KOMART_{TITEL}_LOAD_SP2">
<XHEADER state="1">
<Title>shell_handler</Title>
<ArchiveKey1/>
<ArchiveKey2/>
<Active>1</Active>
<OH_SubType/>
<CustomAttributes KeyListID="0" dataRequestID="0"/>
</XHEADER>
<OUTPUTREG state="1">
<FileReg/>
</OUTPUTREG>
<SYNCREF state="1">
<Syncs/>
</SYNCREF>
<ATTR_JOBS state="1">
<Queue>CLIENT_QUEUE</Queue>
<StartType/>
<HostDst>|LVSDPEETL021</HostDst>
<HostATTR_Type>UNIX</HostATTR_Type>
<CodeName/>
<Login>LOGIN.ETL.O_KOMART</Login>
<IntAccount/>
<ExtRepDef>1</ExtRepDef>
<ExtRepAll>0</ExtRepAll>
<ExtRepNone>0</ExtRepNone>
<AutoDeactNo>0</AutoDeactNo>
<AutoDeact1ErrorFree>0</AutoDeact1ErrorFree>
<AutoDeactErrorFree>0</AutoDeactErrorFree>
<DeactWhen/>
<DeactDelay>0</DeactDelay>
<AutoDeactAlways>1</AutoDeactAlways>
<AttDialog>0</AttDialog>
<ActAtRun>0</ActAtRun>
<Consumption>0</Consumption>
<UC4Priority>0</UC4Priority>
<MaxParallel2>0</MaxParallel2>
<MpElse1>1</MpElse1>
<MpElse2>0</MpElse2>
<TZ/>
</ATTR_JOBS>
<ATTR_UNIX state="1">
<OutputDb>1</OutputDb>
<OutputDbErr>0</OutputDbErr>
<OutputFile>0</OutputFile>
<ShellScript>1</ShellScript>
<Command>0</Command>
<Priority>0</Priority>
<Shell>-ksh</Shell>
<ShellOptions/>
<Com/>
</ATTR_UNIX>
<RUNTIME state="1">
<MaxRetCode>0</MaxRetCode>
<MrcExecute/>
<MrcElseE>0</MrcElseE>
<FcstStatus>0| |</FcstStatus>
<Ert>3</Ert>
<ErtMethodDef>1</ErtMethodDef>
<ErtMethodFix>0</ErtMethodFix>
<ErtFix>0</ErtFix>
<ErtDynMethod>2|Average</ErtDynMethod>
<ErtMethodDyn>0</ErtMethodDyn>
<ErtCnt>0</ErtCnt>
<ErtCorr>0</ErtCorr>
<ErtIgn>0</ErtIgn>
<ErtIgnFlg>0</ErtIgnFlg>
<ErtMinCnt>0</ErtMinCnt>
<MrtMethodNone>1</MrtMethodNone>
<MrtMethodFix>0</MrtMethodFix>
<MrtFix>0</MrtFix>
<MrtMethodErt>0</MrtMethodErt>
<MrtErt>0</MrtErt>
<MrtMethodDate>0</MrtMethodDate>
<MrtDays>0</MrtDays>
<MrtTime>00:00</MrtTime>
<MrtTZ/>
<SrtMethodNone>1</SrtMethodNone>
<SrtMethodFix>0</SrtMethodFix>
<SrtFix>0</SrtFix>
<SrtMethodErt>0</SrtMethodErt>
<SrtErt>0</SrtErt>
<MrtCancel>0</MrtCancel>
<MrtExecute>0</MrtExecute>
<MrtExecuteObj/>
</RUNTIME>
<DYNVALUES state="1">
<dyntree>
<node content="1" id="VALUE" name="Variables" parent="" type="VALUE">
<VALUE state="1">
<Values/>
<Mode>0</Mode>
</VALUE>
</node>
</dyntree>
</DYNVALUES>
<ROLLBACK state="1">
<RollbackFlag>0</RollbackFlag>
<CBackupObj/>
<CRollbackObj/>
<FBackupPath/>
<FDeleteBefore>0</FDeleteBefore>
<FInclSubDirs>0</FInclSubDirs>
</ROLLBACK>
<PRE_SCRIPT mode="1" replacementmode="1" state="1">
<PSCRI/>
</PRE_SCRIPT>
<SCRIPT mode="1" state="1">
<MSCRI><![CDATA[$DW_EXE/shell_handler.ksh {ETL_ID} sp2 $DW_EXE/non_uow_ex_files_upload_to_hdfs.ksh {ETL_ID} sp2 &today# /sys/edw/working/komart/komart_w {STG_TABLE_NAME} ko]]></MSCRI>
</SCRIPT>
<OUTPUTSCAN state="1">
<Inherit>N</Inherit>
<filterobjects/>
<HostFsc/>
<LoginFsc/>
</OUTPUTSCAN>
<POST_SCRIPT mode="1" replacementmode="1" state="1">
<OSCRI><![CDATA[!************************** Chain / Module Conditions  **************************

!
!************************** Chain / Module Arguments **************************]]></OSCRI>
</POST_SCRIPT>
<DOCU_Docu state="1" type="text">
<DOC/>
</DOCU_Docu>
</JOBS_UNIX>

'''



output_location = os.getcwd()
output_file1 = output_location + '\\ko_ste_xml.txt'


if os.path.exists(output_file1):
    os.remove(output_file1)

index = 0
seq_list = ['o_komart.auction_third2_ad','o_komart.byr_grade_gmkt','o_komart.byr_grade_iac','o_komart.byr_group_gmkt','o_komart.byr_group_iac','o_komart.byr_issue_gmkt','o_komart.byr_use_gmkt','o_komart.byr_use_iac','o_komart.cac_kor_dim_c2c_yn','o_komart.cac_kor_dim_item_g_new','o_komart.cac_kor_dim_item_i_new','o_komart.cac_kor_dim_item_reg_method_g','o_komart.cac_kor_dim_item_reg_method_i','o_komart.cac_kor_dim_os2_catalog_g','o_komart.cac_kor_dim_os2_catalog_i','o_komart.cac_kor_dim_pay_way_g','o_komart.cac_kor_dim_premium_partner_yn','o_komart.cac_kor_dim_sell_grade_i','o_komart.cac_kor_dim_smile_pay_yn','o_komart.cac_kor_dim_spmarket_yn','o_komart.cac_kor_dim_used_yn','o_komart.cac_kor_efm_wh_info_iac','o_komart.cac_kor_gmkt_fact_pay','o_komart.cac_kor_iac_fact_pay','o_komart.gmarket_third_ad','o_komart.gmarket_third2_ad']
seq_list2 = ['stg_auction_third2_ad_w','stg_byr_grade_gmkt_w','stg_byr_grade_iac_w','stg_byr_group_gmkt_w','stg_byr_group_iac_w','stg_byr_issue_gmkt_w','stg_byr_use_gmkt_w','stg_byr_use_iac_w','stg_cac_kor_dim_c2c_yn_w','stg_cac_kor_dim_item_g_new_w','stg_cac_kor_dim_item_i_new_w','stg_cac_kor_dim_item_med_g_w','stg_cac_kor_dim_item_reg_i_w','stg_cac_kor_dim_os2_ctlg_g_w','stg_cac_kor_dim_os2_ctlg_i_w','stg_cac_kor_dim_pay_way_g_w','stg_cac_kor_dim_premium_w','stg_cac_kor_dim_sell_grade_i_w','stg_cac_kor_dim_smile_pay_yn_w','stg_cac_kor_dim_spmarket_yn_w','stg_cac_kor_dim_used_yn_w','STG_CAC_KOR_EFM_IAC_W','stg_cac_kor_gmkt_fact_pay_w','stg_cac_kor_iac_fact_pay_w','stg_gmarket_third_ad_w','stg_gmarket_third2_ad_w']


for seq in seq_list:
    etl_id = seq_list[index]
    stg_table = seq_list2[index]
    title = str(etl_id).split('.')[1]
    output = seq_str1.format(TITEL=title, ETL_ID=etl_id, STG_TABLE_NAME=stg_table)
    write_to_file(output_file1, output)
    index = index + 1
# while start_value <= end_value:
#     value1 = start_value.__format__('%Y-%m-%d')
#     start_value = start_value + datetime.timedelta(1)
#     value2 = start_value.__format__('%Y-%m-%d')
#     #print(index, value1, value2)
#     output_str1 = seq_str1
#     output_str1 = output_str1.format(INDEX=index, START_VALUE=value1, END_VALUE=value2)
#
#     output_str2 = seq_str2
#     output_str2 = output_str2.format(INDEX=index, START_VALUE=value1, END_VALUE=value2)
#
#     output_str3 = seq_str3
#     output_str3 = output_str3.format(INDEX=index)
#
#     output_str4 = seq_str4
#     output_str4 = output_str4.format(INDEX=index)
#
#     index = index + 1
#     write_to_file(output_file1, output_str1)
#     write_to_file(output_file2, output_str2)
#     write_to_file(output_file3, output_str3)
#     write_to_file(output_file4, output_str4)

