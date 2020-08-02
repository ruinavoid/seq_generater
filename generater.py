import os
import datetime


def write_to_file(file_path, write_str):
    file_writer = open(file_path, 'ab+')
    file_writer.write(write_str.encode("utf-8"))
    file_writer.close()


seq_str1 = '''
alter table zeta_dev_working.stg_hadoop_item_aspct_cl_w_sanwu add partition (yy='2020',mm='04',dd='{INDEX}',hh='{HH}') location 'hdfs://hercules/sys/edw/item_aspct_clssfctn/working/item_aspct_clss_ngc/2020/04/{INDEX}/{HH}';
'''



output_location = os.getcwd()
output_file1 = output_location + '\\output1.txt'
output_file2 = output_location + '\\output2.txt'
output_file3 = output_location + '\\output3.txt'
output_file4 = output_location + '\\output4.txt'

if os.path.exists(output_file1):
    os.remove(output_file1)
if os.path.exists(output_file2):
    os.remove(output_file2)
if os.path.exists(output_file3):
    os.remove(output_file3)

start_value = datetime.date(2019, 6, 1)
end_value = datetime.date(2019, 8, 1)
index = 1
seq_list = ['CNTRCT_TYPE','DSCNT_PLAN','DSCNT_RATE','DSCNT_TYPE','DW_ACCOUNT_ADJ_REASON_CODE','DW_ACCT_ADJ_TYPE_LKP','DW_ACCT_CRDT_LAG','ACCT_QLFD_SLR_DSCNT_RECON_SM']

# for seq in seq_list:
#     output = seq_str1.format(INDEX=seq)
#     write_to_file(output_file1, output)

for seq in range(1,6):
    for hh in '00','03','06','09','12','15','18','21':

        output = seq_str1.format(INDEX=str(seq).zfill(2),HH=hh)
        write_to_file(output_file1, output)

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

