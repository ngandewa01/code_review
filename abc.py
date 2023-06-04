def getdlmacstats(log_path, kpitype = "dl", kpii = "all", **kwargs):
    """
    API to get downlink mac stats from enb logs.
    the log name should start with enb.*.

    :param str directory_path : path for enb logs.
    :param str kpitype : DL|UL, Default is DL.
    :param str kpi : MCS|CQI|PMI|BLER|RI|ALL, Default is ALL.

    :return:
        MAC stats are populated in excel tbsltekpi.xlsx in
        current directory.
    """
    time.sleep(1)
    try:
        config_log()
        pplogger = logging.getLogger()
        directory_path = os.path.dirname(log_path)
        print(directory_path)

        pplogger.info(
            f"Calling downlink kpi-stats and passing arguments  directory_path={log_path}, kpitype={kpitype} , kpi={kpii}")
        cell_s = re.compile('(\d+:\d+:\d+)\w+.*(CELL-(\d+)).*')
        rank_index = re.compile(
            '(\d+:\d+:\d+).*LM_DEBUG ENBC_RTF_CQ_REPORT_MGR .*(CELL-(\d+)).*Rx CQ.* RI\((\d+)\).*RNTI\((\d+)\)')
        ra_index = re.compile(
            '(\d+:\d+:\d+).\d+ \d*\s*\d*\s*LM_DEBUG ENBC_RTF_UE_CONTEXT \d*\s*CELL-(\d+) DlUeContext::prepareDlSchedulingParams:RNTI\((\d+)\) csiprocindex\(\d+\) pqiset\(\d+\) tm\(\d+\) dciformat\(\d+\) tbCount\(\d+\) RI\((\d+)\) MaxLayersPossible\(\d+\) MimoOperationMode\(\d+\)')
        cqi = re.compile(
            '(\d+:\d+:\d+).*LM_DEBUG ENBC_RTF_CQ_REPORT_MGR .*CELL-(\d+).*Setting wideband CQI: CwIndex \((\d+)\) CQI \((\d+)\).*RNTI\((\d+)\)')
        cqi_one = re.compile(
            '(\d+:\d+:\d+).*LM_DEBUG ENBC_RTF_CQ_REPORT_MGR \d*\s*CELL-(\d+) Rx CQ periodic report :Type 2:\[\d+, \d+\] Setting wideband cqi for codeword \((\d+)\) spatial differential cqi value \(\d+\), computed cqi value \((\d+)\), RNTI\((\d+)\)')
        cqi_two = re.compile(
            "(\d+:\d+:\d+).*LM_DEBUG ENBC_RTF_CQ_REPORT_MGR \d*\s*CELL-(\d+) Rx CQ periodic report :Type 2:\[\d+, \d+\] Setting wideband cqi for codeword \((\d+)\) value \((\d+)\), RNTI\((\d+)\)")
        mtput_0 = re.compile(
            "(\d+:\d+:\d+\.\d{,3}).*LM_DEBUG ENBC_RTF_UCI_PROCESSOR \d*\s*CELL-(\d+) \[\d+, \d+\]:\[\d+, \d+\]:\[C-RNTI:(\d+)\]:Rx UlAckData:CwCount\((\d+)\) HarqResult\((\d+):(\d+)\) HarqProcessId\((\d+)\) Tbsize\((\d+):(\d+)\) TxCount\((\d+):(\d+)\)")
        stput_0 = re.compile(
            "(\d+:\d+:\d+.\d+) \d*\s*\d*\s*LM_DEBUG ENBC_RTF_PDSCH_ASSIGNMENT (\d*)\s*CELL-(\d+) PDSCH Assignment::show TxTimepoint\[(\d+), (\d+)\] RNTI:\((\d+)\):TB assignment\[(\d+)\]: HARQ Result\(\d+\) TBS\((\d+)\) MCS\((\d+)\) NDI\((\d+)\) RV\((\d+)\) LayerBitmap\((\d+)\), newTxFlag\(\d+\), RbCount\((\d+)\) TbCount\((\d+)\), HarqId\((\d+)\), dciFormat\((\d+)\)")
        mesure_tput = re.compile(
            "(\d+:\d+:\d+\.\d{,3}).*LM_DEBUG ENBC_RTF_UCI_PROCESSOR \d*\s*CELL-(\d+) \[\d+, \d+\]:\[\d+, \d+\]:\[C-RNTI:(\d+)\]:Rx UlAckData:CwCount\((\d+)\) HarqResult\((\d+):(\d+)\) HarqProcessId\((\d+)\) Tbsize\((\d+):(\d+)\) TxCount\((\d+):(\d+)\)")
        mcs_0 = re.compile(
            '(\d+:\d+:\d+).*LM_DEBUG ENBC_RTF_CQ_REPORT_MGR \d*\s*CELL-(\d+) Updating MCS: Success: CwIndex\((\d+)\) WidebandCqi\(\d+\) McsIndex\((\d+)\), RNTI\((\d+)\)')
        pmi_0 = re.compile(
            "(\d+:\d+:\d+).*LM_DEBUG ENBC_RTF_CQ_REPORT_MGR \d*\s*CELL-(\d+) Rx CQ periodic report :Type 2: Setting PMI: PMI\((\d+)\), RNTI\((\d+)\), csiProcIndex\(\d+\), csiSetIndex\(\d+\)")
        pmi_1 = re.compile(
            "(\d+:\d+:\d+).*LM_DEBUG ENBC_RTF_CQ_REPORT_MGR \d*\s*CELL-(\d+) AperiodicCqReportingMode\d+::processReportedPmi:\[\d+,\d+\] Setting PMI: RNTI\((\d+)\), PMI\((\d+)\)")
        # try:
        #     os.remove(directory_path + '\\' + 'Mac_Stats_Results_DL_9_3_001.csv')
        # except os.error:
        #     pass

        basename = os.path.basename(log_path)
        file_name = os.path.splitext(basename)[0]

        file_path = []
        cell = []
        kpi = {}
        if basename.startswith("enb"):
            with open(log_path, ) as fh:
                print("im here ")
                for read_file in fh:
                    data = read_file.strip()
                    temp = cell_s.match(data)
                    if temp:
                        cell = int(temp.group(3))
                        if cell not in kpi:
                            ri_re = rank_index.match(data)
                            if ri_re:
                                crnti = int(ri_re.group(5))
                                kpi[cell] = {}
                                kpi[cell][crnti] = {}
                                kpi[cell][crnti]['curent_RI'] = int(ri_re.group(4))
                            re_ri = ra_index.match(data)
                            if re_ri:
                                crnti = int(re_ri.group(3))
                                kpi[cell] = {}
                                kpi[cell][crnti] = {}
                                kpi[cell][crnti]['curent_RI'] = int(re_ri.group(4))
                        else:
                            ri_re = rank_index.match(data)
                            if ri_re:
                                kpi[cell][crnti]['curent_RI'] = int(ri_re.group(4))
                                if 'RI' in kpi[cell][crnti]:
                                    kpi[cell][crnti]['RI']['total'] += int(ri_re.group(4))
                                    kpi[cell][crnti]['RI']['count'] += 1
                                else:
                                    kpi[cell][crnti]['RI'] = {}
                                    kpi[cell][crnti]['RI']['total'] = int(ri_re.group(4))
                                    kpi[cell][crnti]['RI']['count'] = 0
                            re_ri = ra_index.match(data)
                            if re_ri:
                                kpi[cell][crnti]['curent_RI'] = int(re_ri.group(4))
                                if 'RI' in kpi[cell][crnti]:
                                    kpi[cell][crnti]['RI']['total'] += int(re_ri.group(4))
                                    kpi[cell][crnti]['RI']['count'] += 1
                                else:
                                    kpi[cell][crnti]['RI'] = {}
                                    kpi[cell][crnti]['RI']['total'] = int(re_ri.group(4))
                                    kpi[cell][crnti]['RI']['count'] = 0
                            cqi_re = cqi.match(data)
                            if cqi_re:
                                CELL_ID = int(cqi_re.group(2))
                                C_RNTI = int(cqi_re.group(5))
                                Cw_Index = int(cqi_re.group(3))
                                cq = int(cqi_re.group(4))
                                if CELL_ID in kpi and C_RNTI in kpi[CELL_ID]:
                                    current_RI = kpi[CELL_ID][C_RNTI]['curent_RI']
                                    if "CQI" not in kpi[CELL_ID][C_RNTI]:
                                        kpi[CELL_ID][C_RNTI]['CQI'] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                                    elif f"Rank{current_RI}_CW{Cw_Index}" in kpi[CELL_ID][C_RNTI]['CQI']:
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] += cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] += 1
                                    else:
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                            cqi1_re = cqi_one.match(data)
                            if cqi1_re:
                                CELL_ID = int(cqi1_re.group(2))
                                C_RNTI = int(cqi1_re.group(5))
                                Cw_Index = int(cqi1_re.group(3))
                                cq = int(cqi1_re.group(4))
                                if CELL_ID in kpi and C_RNTI in kpi[CELL_ID]:
                                    current_RI = kpi[CELL_ID][C_RNTI]['curent_RI']
                                    if "CQI" not in kpi[CELL_ID][C_RNTI]:
                                        kpi[CELL_ID][C_RNTI]['CQI'] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                                    elif f"Rank{current_RI}_CW{Cw_Index}" in kpi[CELL_ID][C_RNTI]['CQI']:
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] += cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] += 1
                                    else:
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                            cqi2_re = cqi_two.match(data)
                            if cqi2_re:
                                CELL_ID = int(cqi2_re.group(2))
                                C_RNTI = int(cqi2_re.group(5))
                                Cw_Index = int(cqi2_re.group(3))
                                cq = int(cqi2_re.group(4))
                                if CELL_ID in kpi and C_RNTI in kpi[CELL_ID]:
                                    current_RI = kpi[CELL_ID][C_RNTI]['curent_RI']
                                    if "CQI" not in kpi[CELL_ID][C_RNTI]:
                                        kpi[CELL_ID][C_RNTI]['CQI'] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                                    elif f"Rank{current_RI}_CW{Cw_Index}" in kpi[CELL_ID][C_RNTI]['CQI']:
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] += cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] += 1
                                    else:
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = cq
                                        kpi[CELL_ID][C_RNTI]['CQI'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                            m_tput = mtput_0.match(data)
                            if m_tput:
                                CELL_ID = int(m_tput.group(2))
                                C_RNTI = int(m_tput.group(3))
                                CWCount = int(m_tput.group(4))
                                Harq_CW0 = int(m_tput.group(5))
                                Harq_CW1 = int(m_tput.group(6))
                                HarqID = int(m_tput.group(7))
                                TBSize_CW0 = int(m_tput.group(8))
                                TBSize_CW1 = int(m_tput.group(9))
                                TxCount_CW0 = int(m_tput.group(10))
                                TxCount_CW1 = int(m_tput.group(11))
                                if CELL_ID in kpi and C_RNTI in kpi[CELL_ID]:
                                    current_RI = kpi[CELL_ID][C_RNTI]['curent_RI']
                                    if 'TxBLER' not in kpi[CELL_ID][C_RNTI]:
                                        kpi[CELL_ID][C_RNTI]['TxBLER'] = {}
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"] = {}
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                            'total_Harq_CW0'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"] = {}
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"][
                                            'total_Harq_CW1'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"]['Harq_CW0'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['Harq_CW1'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"]['nack_cw0'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['nack_cw1'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"]['DTX'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['DTX'] = 0
                                    elif f"Rank{current_RI}_CW0" in kpi[CELL_ID][C_RNTI]['TxBLER']:
                                        if CWCount == 1:
                                            if TxCount_CW0 == 1:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                                    'total_Harq_CW0'] += 1
                                            if Harq_CW0 == 1:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                                    'Harq_CW0'] += 1
                                            else:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                                    'nack_cw0'] += 1
                                            if Harq_CW0 == 2:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"]['DTX'] += 1
                                        if CWCount == 2:
                                            if TxCount_CW0 == 1:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                                    'total_Harq_CW0'] += 1
                                            if TxCount_CW1 == 1:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"][
                                                    'total_Harq_CW1'] += 1
                                            if Harq_CW0 == 1:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                                    'Harq_CW0'] += 1
                                            else:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                                    'nack_cw0'] += 1
                                            if Harq_CW1 == 1:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"][
                                                    'Harq_CW1'] += 1
                                            else:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"][
                                                    'nack_cw1'] += 1
                                            if Harq_CW0 == 2:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['DTX'] += 1
                                            if Harq_CW1 == 2:
                                                kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['DTX'] += 1
                                    else:
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"] = {}
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"][
                                            'total_Harq_CW0'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"] = {}
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"][
                                            'total_Harq_CW1'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"]['Harq_CW0'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['Harq_CW1'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"]['nack_cw0'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['nack_cw1'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW0"]['DTX'] = 0
                                        kpi[CELL_ID][C_RNTI]['TxBLER'][f"Rank{current_RI}_CW1"]['DTX'] = 0
                            s_tput = stput_0.match(data)
                            if s_tput:
                                CELL_ID = int(s_tput.group(3))
                                C_RNTI = int(s_tput.group(6))
                                Cw_Index = int(s_tput.group(7))
                                stput = int(s_tput.group(8))
                                if CELL_ID in kpi and C_RNTI in kpi[CELL_ID]:
                                    current_RI = kpi[CELL_ID][C_RNTI]['curent_RI']
                                    if "ScheduledTput" not in kpi[CELL_ID][C_RNTI]:
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'] = {}
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"]["total"] = stput
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"]["count"] = 1
                                    elif f"CW{Cw_Index}" in kpi[CELL_ID][C_RNTI]['ScheduledTput']:
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"]["total"] += stput
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"]["count"] += 1
                                    else:
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"]["total"] = stput
                                        kpi[CELL_ID][C_RNTI]['ScheduledTput'][f"CW{Cw_Index}"]["count"] = 1
                            mtput = mesure_tput.match(data)
                            if mtput:
                                CELL_ID = int(mtput.group(2))
                                C_RNTI = int(mtput.group(3))
                                CWCount = int(mtput.group(4))
                                Harq_CW0 = int(mtput.group(5))
                                Harq_CW1 = int(mtput.group(6))
                                HarqID = int(mtput.group(7))
                                TBSize_CW0 = int(mtput.group(8))
                                TBSize_CW1 = int(mtput.group(9))
                                TxCount_CW0 = int(mtput.group(10))
                                TxCount_CW1 = int(mtput.group(11))
                                if CELL_ID in kpi and C_RNTI in kpi[CELL_ID]:
                                    if "MeasTput" in kpi[CELL_ID][C_RNTI]:
                                        if CWCount == 1:
                                            if Harq_CW0 == 1:
                                                kpi[CELL_ID][C_RNTI]['MeasTput']["CW0_total"] += TBSize_CW0
                                                kpi[CELL_ID][C_RNTI]['MeasTput']['CodeWord0_total'] += TBSize_CW0
                                            else:
                                                kpi[CELL_ID][C_RNTI]['failure']['count'] += 1
                                        if CWCount == 2:
                                            if Harq_CW0 == 1:
                                                # MeasTput
                                                kpi[CELL_ID][C_RNTI]['MeasTput']["CW0_total"] += TBSize_CW0
                                                kpi[CELL_ID][C_RNTI]['MeasTput']['CodeWord0_total'] += TBSize_CW0
                                            else:
                                                kpi[CELL_ID][C_RNTI]['failure']['count'] += 1
                                            if Harq_CW1 == 1:
                                                # MeasTput
                                                kpi[CELL_ID][C_RNTI]['MeasTput']["CW1_total"] += TBSize_CW1
                                                kpi[CELL_ID][C_RNTI]['MeasTput']['CodeWord1_total'] += TBSize_CW1
                                            else:
                                                kpi[CELL_ID][C_RNTI]['failure']['count'] += 1
                                    else:
                                        kpi[CELL_ID][C_RNTI]['MeasTput'] = {}
                                        kpi[CELL_ID][C_RNTI]['MeasTput']["CW0_total"] = TBSize_CW0
                                        kpi[CELL_ID][C_RNTI]['MeasTput']["CW1_total"] = TBSize_CW1
                                        kpi[CELL_ID][C_RNTI]['MeasTput']['CodeWord0_total'] = TBSize_CW0
                                        kpi[CELL_ID][C_RNTI]['MeasTput']['CodeWord1_total'] = TBSize_CW1
                                        kpi[CELL_ID][C_RNTI]['failure'] = {}
                                        kpi[CELL_ID][C_RNTI]['failure']['count'] = 1
                            mcs_re = mcs_0.match(data)
                            if mcs_re:
                                CELL_ID = int(mcs_re.group(2))
                                C_RNTI = int(mcs_re.group(5))
                                Cw_Index = int(mcs_re.group(3))
                                Mcs = int(mcs_re.group(4))
                                if CELL_ID in kpi and C_RNTI in kpi[CELL_ID]:
                                    current_RI = kpi[CELL_ID][C_RNTI]['curent_RI']
                                    if "MCS" not in kpi[CELL_ID][C_RNTI]:
                                        kpi[CELL_ID][C_RNTI]['MCS'] = {}
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = Mcs
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                                    elif f"Rank{current_RI}_CW{Cw_Index}" in kpi[CELL_ID][C_RNTI]['MCS']:
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"][
                                            "total"] += Mcs
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] += 1
                                    else:
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"] = {}
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"]["total"] = Mcs
                                        kpi[CELL_ID][C_RNTI]['MCS'][f"Rank{current_RI}_CW{Cw_Index}"]["count"] = 1
                            pmi_re_0 = pmi_0.match(data)
                            if pmi_re_0:
                                CELL_ID = int(cqi2_re.group(2))
                                C_RNTI = int(cqi2_re.group(4))
                                pmi = int(cqi2_re.group(3))
                                if 'PMI' in kpi[cell][crnti]:
                                    kpi[cell][crnti]['PMI']['total'] += pmi
                                    kpi[cell][crnti]['PMI']['count'] += 1
                                else:
                                    kpi[cell][crnti]['PMI'] = {}
                                    kpi[cell][crnti]['PMi']['total'] = pmi
                                    kpi[cell][crnti]['PMI']['count'] = 0
                            pmi_re = pmi_1.match(data)
                            if pmi_re:
                                CELL_ID = int(cqi2_re.group(2))
                                C_RNTI = int(cqi2_re.group(3))
                                pmi = int(cqi2_re.group(4))
                                if 'PMI' in kpi[cell][crnti]:
                                    kpi[cell][crnti]['PMI']['total'] += pmi
                                    kpi[cell][crnti]['PMI']['count'] += 1
                                else:
                                    kpi[cell][crnti]['PMI'] = {}
                                    kpi[cell][crnti]['PMi']['total'] = pmi
                                    kpi[cell][crnti]['PMI']['count'] = 0
                Rank1TxBLERCW0, Rank2TxBLERCW0, Rank2TxBLERCW1 = [], [], []
                Rank3TxBLERCW0, Rank3TxBLERCW1, Rank4TxBLERCW0, Rank4TxBLERCW1 = [], [], [], []
                mcs_rank1, mcs_rank2_cw0, mcs_rank2_cw1, mcs_rank3_cw0 = [], [], [], []
                mcs_rank3_cw1, mcs_rank4_cw0, mcs_rank4_cw1 = [], [], []
                cqi_rank1, cqi_rank2_cw0, cqi_rank2_cw1, cqi_rank3_cw0 = [], [], [], []
                cqi_rank3_cw1, cqi_rank4_cw0, cqi_rank4_cw1 = [], [], []
                RI_T, CRNTI, PMI_t, s_cw0, s_cw1, m_cw0, m_cw1, pdsch_failure, cells = [], [], [], [], [], [], [], [], []
                for cell in kpi.keys():
                    cells.append(cell)
                    file_path.append(basename)
                    for crnti in kpi[cell]:
                        if "RI" in kpi[cell][crnti]:
                            ri_total = kpi[cell][crnti]["RI"]['total']
                            ri_count = kpi[cell][crnti]["RI"]['count']
                            if ri_total:
                                RI_T.append(int(ri_total / ri_count))
                                CRNTI.append(crnti)
                            else:
                                RI_T.append('-')
                        if "CQI" in kpi[cell][crnti]:
                            cqi_total = kpi[cell][crnti]["CQI"]
                            if f"Rank1_CW0" in cqi_total:
                                cqi_1 = kpi[cell][crnti]["CQI"]["Rank1_CW0"]["total"]
                                cqi_1c = kpi[cell][crnti]["CQI"]["Rank1_CW0"]["count"]
                                cqi_rank1.append(cqi_1 / cqi_1c)
                            else:
                                cqi_rank1.append("NA")
                            if f"Rank2_CW0" in cqi_total:
                                cqi_2 = kpi[cell][crnti]["CQI"]["Rank2_CW0"]["total"]
                                cqi_2c = kpi[cell][crnti]["CQI"]["Rank2_CW0"]["count"]
                                cqi_rank2_cw0.append(cqi_2 / cqi_2c)
                            else:
                                cqi_rank2_cw0.append("NA")
                            if f"Rank2_CW1" in cqi_total:
                                cqi_21 = kpi[cell][crnti]["CQI"]["Rank2_CW1"]["total"]
                                cqi_21c = kpi[cell][crnti]["CQI"]["Rank2_CW1"]["count"]
                                cqi_rank2_cw1.append(cqi_21 / cqi_21c)
                            else:
                                cqi_rank2_cw1.append("NA")
                            if f"Rank3_CW0" in cqi_total:
                                cqi_3 = kpi[cell][crnti]["CQI"]["Rank3_CW0"]["total"]
                                cqi_3c = kpi[cell][crnti]["CQI"]["Rank3_CW0"]["count"]
                                cqi_rank3_cw0.append(cqi_3 / cqi_3c)
                            else:
                                cqi_rank3_cw0.append("NA")
                            if f"Rank3_CW1" in cqi_total:
                                cqi_31 = kpi[cell][crnti]["CQI"]["Rank3_CW1"]["total"]
                                cqi_31c = kpi[cell][crnti]["CQI"]["Rank3_CW1"]["count"]
                                cqi_rank3_cw1.append(cqi_31 / cqi_31c)
                            else:
                                cqi_rank3_cw1.append("NA")
                            if f"Rank4_CW0" in cqi_total:
                                cqi_4 = kpi[cell][crnti]["CQI"]["Rank4_CW0"]["total"]
                                cqi_4c = kpi[cell][crnti]["CQI"]["Rank4_CW0"]["count"]
                                cqi_rank4_cw0.append(cqi_4 / cqi_4c)
                            else:
                                cqi_rank4_cw0.append("NA")
                            if f"Rank4_CW1" in cqi_total:
                                cqi_41 = kpi[cell][crnti]["CQI"]["Rank4_CW1"]["total"]
                                cqi_41c = kpi[cell][crnti]["CQI"]["Rank4_CW1"]["count"]
                                cqi_rank4_cw1.append(cqi_41 / cqi_41c)
                            else:
                                cqi_rank4_cw1.append("NA")
                        if "MCS" in kpi[cell][crnti]:
                            mcs_total = kpi[cell][crnti]["MCS"]
                            if f"Rank1_CW0" in mcs_total:
                                mcs_1 = kpi[cell][crnti]["MCS"]["Rank1_CW0"]["total"]
                                mcs_1c = kpi[cell][crnti]["MCS"]["Rank1_CW0"]["count"]
                                mcs_rank1.append(mcs_1 / mcs_1c)
                            else:
                                mcs_rank1.append("NA")
                            if f"Rank2_CW0" in mcs_total:
                                mcs_2 = kpi[cell][crnti]["MCS"]["Rank2_CW0"]["total"]
                                mcs_2c = kpi[cell][crnti]["MCS"]["Rank2_CW0"]["count"]
                                mcs_rank2_cw0.append(mcs_2 / mcs_2c)
                            else:
                                mcs_rank2_cw0.append("NA")
                            if f"Rank2_CW1" in mcs_total:
                                mcs_21 = kpi[cell][crnti]["MCS"]["Rank2_CW1"]["total"]
                                mcs_21c = kpi[cell][crnti]["MCS"]["Rank2_CW1"]["count"]
                                mcs_rank2_cw1.append(mcs_21 / mcs_21c)
                            else:
                                mcs_rank2_cw1.append("NA")
                            if f"Rank3_CW0" in mcs_total:
                                mcs_3 = kpi[cell][crnti]["MCS"]["Rank3_CW0"]["total"]
                                mcs_3c = kpi[cell][crnti]["MCS"]["Rank3_CW0"]["count"]
                                mcs_rank3_cw0.append(mcs_3 / mcs_3c)
                            else:
                                mcs_rank3_cw0.append("NA")
                            if f"Rank3_CW1" in mcs_total:
                                mcs_31 = kpi[cell][crnti]["MCS"]["Rank3_CW1"]["total"]
                                mcs_31c = kpi[cell][crnti]["MCS"]["Rank3_CW1"]["count"]
                                mcs_rank3_cw1.append(mcs_31 / mcs_31c)
                            else:
                                mcs_rank3_cw1.append("NA")
                            if f"Rank4_CW0" in mcs_total:
                                mcs_4 = kpi[cell][crnti]["MCS"]["Rank4_CW0"]["total"]
                                mcs_4c = kpi[cell][crnti]["MCS"]["Rank4_CW0"]["count"]
                                mcs_rank4_cw0.append(mcs_4 / mcs_4c)
                            else:
                                mcs_rank4_cw0.append("NA")
                            if f"Rank4_CW1" in mcs_total:
                                mcs_41 = kpi[cell][crnti]["MCS"]["Rank4_CW1"]["total"]
                                mcs_41c = kpi[cell][crnti]["MCS"]["Rank4_CW1"]["count"]
                                mcs_rank4_cw1.append(mcs_41 / mcs_41c)
                            else:
                                mcs_rank4_cw1.append("NA")
                        totalScheduledcount = 0
                        if "ScheduledTput" in kpi[cell][crnti]:
                            sht = kpi[cell][crnti]['ScheduledTput']
                            if 'CW0' in kpi[cell][crnti]['ScheduledTput']:
                                s_total = kpi[cell][crnti]['ScheduledTput']['CW0']['total']
                                s_count = kpi[cell][crnti]['ScheduledTput']['CW0']['count']
                                s_cw0.append(s_total * 8 / (1000 * s_count))
                                totalScheduledcount += s_count
                                if "MeasTput" in kpi[cell][crnti]:
                                    if 'CodeWord0_total' in kpi[cell][crnti]['MeasTput']:
                                        mput_total = kpi[cell][crnti]['MeasTput']['CodeWord0_total']
                                        try:
                                            m_cw0.append(mput_total * 8 / (1000 * s_count))
                                        except ZeroDivisionError:
                                            m_cw0.append(0)
                            if 'CW1' in kpi[cell][crnti]['ScheduledTput']:
                                s_total_cw1 = kpi[cell][crnti]['ScheduledTput']['CW1']['total']
                                s_count_cw1 = kpi[cell][crnti]['ScheduledTput']['CW1']['count']
                                s_cw1.append(s_total_cw1 * 8 / (1000 * s_count_cw1))
                                totalScheduledcount += s_count_cw1
                                if "MeasTput" in kpi[cell][crnti]:
                                    if 'CodeWord1_total' in kpi[cell][crnti]['MeasTput']:
                                        mput_t = kpi[cell][crnti]['MeasTput']['CodeWord1_total']
                                        try:
                                            m_cw1.append(mput_t * 8 / (1000 * s_count_cw1))
                                        except ZeroDivisionError:
                                            m_cw1.append(0)
                        if "failure" in kpi[cell][crnti]:
                            failures = kpi[cell][crnti]['failure']['count']
                            try:
                                pdsch_failure.append(failures * 100 / totalScheduledcount)
                            except ZeroDivisionError:
                                pdsch_failure.append(0)
                        if "TxBLER" in kpi[cell][crnti]:
                            if 'Rank1_CW0' in kpi[cell][crnti]['TxBLER']:
                                temp_harq = kpi[cell][crnti]['TxBLER']['Rank1_CW0']['total_Harq_CW0']
                                nak_cnt = kpi[cell][crnti]['TxBLER']['Rank1_CW0']['nack_cw0']
                                dtx_cnt = kpi[cell][crnti]['TxBLER']['Rank1_CW0']['DTX']
                                if (dtx_cnt + nak_cnt > 0):
                                    AvgTxBler = 0
                                    if (temp_harq > 0):
                                        AvgTxBler = ((nak_cnt + dtx_cnt) * 100) / temp_harq
                                    kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank1_CW0'] = AvgTxBler
                                    Rank1TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank1_CW0'])
                            else:
                                kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank1_CW0'] = 0
                                Rank1TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank1_CW0'])
                            if 'Rank2_CW0' in kpi[cell][crnti]['TxBLER']:
                                temp_harq = kpi[cell][crnti]['TxBLER']['Rank2_CW0']['total_Harq_CW0']
                                nak_cnt = kpi[cell][crnti]['TxBLER']['Rank2_CW0']['nack_cw0']
                                dtx_cnt = kpi[cell][crnti]['TxBLER']['Rank2_CW0']['DTX']
                                if (dtx_cnt + nak_cnt > 0):
                                    AvgTxBler = 0
                                    if (temp_harq > 0):
                                        AvgTxBler = ((nak_cnt + dtx_cnt) * 100) / temp_harq
                                    kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW0'] = AvgTxBler
                                    Rank2TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW0'])
                            else:
                                kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW0'] = 0
                                Rank2TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW0'])
                            if 'Rank2_CW1' in kpi[cell][crnti]['TxBLER']:
                                temp_harq = kpi[cell][crnti]['TxBLER']['Rank2_CW1']['total_Harq_CW1']
                                nak_cnt = kpi[cell][crnti]['TxBLER']['Rank2_CW1']['nack_cw1']
                                dtx_cnt = kpi[cell][crnti]['TxBLER']['Rank2_CW1']['DTX']
                                if (dtx_cnt + nak_cnt > 0):
                                    AvgTxBler = 0
                                    if (temp_harq > 0):
                                        AvgTxBler = ((nak_cnt + dtx_cnt) * 100) / temp_harq
                                    kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW1'] = AvgTxBler
                                    Rank2TxBLERCW1.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW1'])
                            else:
                                kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW1'] = 0
                                Rank2TxBLERCW1.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank2_CW1'])
                            if 'Rank3_CW0' in kpi[cell][crnti]['TxBLER']:
                                temp_harq = kpi[cell][crnti]['TxBLER']['Rank3_CW0']['total_Harq_CW0']
                                nak_cnt = kpi[cell][crnti]['TxBLER']['Rank3_CW0']['nack_cw0']
                                dtx_cnt = kpi[cell][crnti]['TxBLER']['Rank3_CW0']['DTX']
                                if (dtx_cnt + nak_cnt > 0):
                                    AvgTxBler = 0
                                    if (temp_harq > 0):
                                        AvgTxBler = ((nak_cnt + dtx_cnt) * 100) / temp_harq
                                    kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW0'] = AvgTxBler
                                    Rank3TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW0'])
                            else:
                                kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW0'] = 0
                                Rank3TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW0'])
                            if 'Rank3_CW1' in kpi[cell][crnti]['TxBLER']:
                                temp_harq = kpi[cell][crnti]['TxBLER']['Rank3_CW1']['total_Harq_CW1']
                                nak_cnt = kpi[cell][crnti]['TxBLER']['Rank3_CW1']['nack_cw1']
                                dtx_cnt = kpi[cell][crnti]['TxBLER']['Rank3_CW1']['DTX']
                                if (dtx_cnt + nak_cnt > 0):
                                    AvgTxBler = 0
                                    if (temp_harq > 0):
                                        AvgTxBler = ((nak_cnt + dtx_cnt) * 100) / temp_harq
                                    kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW1'] = AvgTxBler
                                    Rank3TxBLERCW1.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW1'])
                            else:
                                kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW1'] = 0
                                Rank3TxBLERCW1.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank3_CW1'])
                            if 'Rank4_CW0' in kpi[cell][crnti]['TxBLER']:
                                temp_harq = kpi[cell][crnti]['TxBLER']['Rank4_CW0']['total_Harq_CW0']
                                nak_cnt = kpi[cell][crnti]['TxBLER']['Rank4_CW0']['nack_cw0']
                                dtx_cnt = kpi[cell][crnti]['TxBLER']['Rank4_CW0']['DTX']
                                if (dtx_cnt + nak_cnt > 0):
                                    AvgTxBler = 0
                                    if (temp_harq > 0):
                                        AvgTxBler = ((nak_cnt + dtx_cnt) * 100) / temp_harq
                                    kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW0'] = AvgTxBler
                                    Rank4TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW0'])
                            else:
                                kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW0'] = 0
                                Rank4TxBLERCW0.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW0'])
                            if 'Rank4_CW1' in kpi[cell][crnti]['TxBLER']:
                                temp_harq = kpi[cell][crnti]['TxBLER']['Rank4_CW1']['total_Harq_CW1']
                                nak_cnt = kpi[cell][crnti]['TxBLER']['Rank4_CW1']['nack_cw1']
                                dtx_cnt = kpi[cell][crnti]['TxBLER']['Rank4_CW1']['DTX']
                                if (dtx_cnt + nak_cnt > 0):
                                    AvgTxBler = 0
                                    if (temp_harq > 0):
                                        AvgTxBler = ((nak_cnt + dtx_cnt) * 100) / temp_harq
                                    kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW1'] = AvgTxBler
                                    Rank4TxBLERCW1.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW1'])
                            else:
                                kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW1'] = 0
                                Rank4TxBLERCW1.append(kpi[cell][crnti]['TxBLER']['1stTX_BLER_Rank4_CW1'])
                        if "PMI" in kpi[cell][crnti]:
                            pmi_count = kpi[cell][crnti]["PMI"]['total']
                            pmi_total = kpi[cell][crnti]["PMI"]['count']
                            if pmi_total:
                                PMI_t.append(pmi_count / pmi_total)
                            else:
                                PMI_t.append('NA')
                if kpii.lower() == "ri":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df["Mean Rank"] = pd.Series(RI_T)

                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)
                    RI_T = []
                if kpii.lower() == "crnti":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df['crnti'] = pd.Series(CRNTI)
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)
                    CRNTI = []
                if kpii.lower() == "cqi":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df['Mean CQI (Rank1 codeword0)'] = pd.Series(cqi_rank1)
                    df['Mean CQI (Rank2 codeword0)'] = pd.Series(cqi_rank2_cw0)
                    df['Mean CQI (Rank2 codeword1)'] = pd.Series(cqi_rank2_cw1)
                    df['Mean CQI (Rank3 codeword0)'] = pd.Series(cqi_rank3_cw0)
                    df['Mean CQI (Rank3 codeword1)'] = pd.Series(cqi_rank3_cw1)
                    df['Mean CQI (Rank4 codeword0)'] = pd.Series(cqi_rank4_cw0)
                    df['Mean CQI (Rank4 codeword1)'] = pd.Series(cqi_rank4_cw1)
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)

                    cqi_rank1 = []
                    cqi_rank2_cw0 = []
                    cqi_rank2_cw1 = []
                    cqi_rank3_cw0 = []
                    cqi_rank3_cw1 = []
                    cqi_rank4_cw0 = []
                    cqi_rank4_cw1 = []
                if kpii.lower() == "mcs":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df['Mean MCS (Rank1 codeword0)'] = pd.Series(mcs_rank1)
                    df['Mean MCS (Rank2 codeword0)'] = pd.Series(mcs_rank2_cw0)
                    df['Mean MCS (Rank2 codeword1)'] = pd.Series(mcs_rank2_cw1)
                    df['Mean MCS (Rank3 codeword0)'] = pd.Series(mcs_rank3_cw0)
                    df['Mean MCS (Rank3 codeword1)'] = pd.Series(mcs_rank3_cw1)
                    df['Mean MCS (Rank4 codeword0)'] = pd.Series(mcs_rank4_cw0)
                    df['Mean MCS (Rank4 codeword1)'] = pd.Series(mcs_rank4_cw1)
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)
                    mcs_rank1 = []
                    mcs_rank2_cw0 = []
                    mcs_rank2_cw1 = []
                    mcs_rank3_cw0 = []
                    mcs_rank3_cw1 = []
                    mcs_rank4_cw0 = []
                    mcs_rank4_cw1 = []
                if kpii.lower() == "bler":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df["1stTX_BLER_Rank1_CW0"] = pd.Series(Rank1TxBLERCW0)
                    df["1stTX_BLER_Rank2_CW0"] = pd.Series(Rank2TxBLERCW0)
                    df["1stTX_BLER_Rank2_CW1"] = pd.Series(Rank2TxBLERCW1)
                    df["1stTX_BLER_Rank3_CW0"] = pd.Series(Rank3TxBLERCW0)
                    df["1stTX_BLER_Rank3_CW1"] = pd.Series(Rank3TxBLERCW1)
                    df["1stTX_BLER_Rank4_CW0"] = pd.Series(Rank4TxBLERCW0)
                    df["1stTX_BLER_Rank4_CW1"] = pd.Series(Rank4TxBLERCW1)
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)
                    cells, file_path = [], []
                    Rank1TxBLERCW0 = []
                    Rank2TxBLERCW0 = []
                    Rank2TxBLERCW1 = []
                    Rank3TxBLERCW0 = []
                    Rank3TxBLERCW1 = []
                    Rank4TxBLERCW0 = []
                    Rank4TxBLERCW1 = []
                if kpii.lower() == "OverallBLER":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df["OverallBLER"] = pd.Series(pdsch_failure)
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)
                    cells, file_path = [], []
                    pdsch_failure = []
                if kpii.lower() == "mtput":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df["Measured Tput CW0"] = pd.Series(m_cw0)
                    df["Measured Tput CW1"] = pd.Series(m_cw1)
                    df["MAC Level DL TPUT (Mbps)"] = (df["Measured Tput CW0"] + df["Measured Tput CW1"]) / 2

                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)

                    cells, file_path = [], []
                    m_cw0, m_cw1 = [], []
                if kpii.lower() == "stput":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df["Scheduled Tput CW0"] = pd.Series(s_cw0)
                    df["Scheduled Tput CW1"] = pd.Series(s_cw1)
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)
                    cells, file_path = [], []
                    s_cw0, s_cw1 = [], []
                if kpii.lower() == "pmi":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df["pmi"] = pd.Series(PMI_t)
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)
                    cells, file_path = [], []
                    PMI_t = []
                if kpii.lower() == "all":
                    df = pd.DataFrame([], columns = [])
                    df['filename'] = file_path
                    df["cell"] = pd.Series(cells)
                    df['crnti'] = pd.Series(CRNTI)
                    df['Mean CQI (Rank1 codeword0)'] = pd.Series(cqi_rank1)
                    df['Mean CQI (Rank2 codeword0)'] = pd.Series(cqi_rank2_cw0)
                    df['Mean CQI (Rank2 codeword1)'] = pd.Series(cqi_rank2_cw1)
                    df['Mean CQI (Rank3 codeword0)'] = pd.Series(cqi_rank3_cw0)
                    df['Mean CQI (Rank3 codeword1)'] = pd.Series(cqi_rank3_cw1)
                    df['Mean CQI (Rank4 codeword0)'] = pd.Series(cqi_rank4_cw0)
                    df['Mean CQI (Rank4 codeword1)'] = pd.Series(cqi_rank4_cw1)
                    df["Mean Rank"] = pd.Series(RI_T)
                    df["1stTX_BLER_Rank1_CW0"] = pd.Series(Rank1TxBLERCW0)
                    df["1stTX_BLER_Rank2_CW0"] = pd.Series(Rank2TxBLERCW0)
                    df["1stTX_BLER_Rank2_CW1"] = pd.Series(Rank2TxBLERCW1)
                    df["1stTX_BLER_Rank3_CW0"] = pd.Series(Rank3TxBLERCW0)
                    df["1stTX_BLER_Rank3_CW1"] = pd.Series(Rank3TxBLERCW1)
                    df["1stTX_BLER_Rank4_CW0"] = pd.Series(Rank4TxBLERCW0)
                    df["1stTX_BLER_Rank4_CW1"] = pd.Series(Rank4TxBLERCW1)
                    df["OverallBLER"] = pd.Series(pdsch_failure)
                    df['Mean MCS (Rank1 codeword0)'] = pd.Series(mcs_rank1)
                    df['Mean MCS (Rank2 codeword0)'] = pd.Series(mcs_rank2_cw0)
                    df['Mean MCS (Rank2 codeword1)'] = pd.Series(mcs_rank2_cw1)
                    df['Mean MCS (Rank3 codeword0)'] = pd.Series(mcs_rank3_cw0)
                    df['Mean MCS (Rank3 codeword1)'] = pd.Series(mcs_rank3_cw1)
                    df['Mean MCS (Rank4 codeword0)'] = pd.Series(mcs_rank4_cw0)
                    df['Mean MCS (Rank4 codeword1)'] = pd.Series(mcs_rank4_cw1)
                    df["Scheduled Tput CW0"] = pd.Series(s_cw0)
                    df["Scheduled Tput CW1"] = pd.Series(s_cw1)
                    df["Measured Tput CW0"] = pd.Series(m_cw0)
                    df["Measured Tput CW1"] = pd.Series(m_cw1)
                    df["MAC Level DL TPUT (Mbps)"] = (df["Measured Tput CW0"] + df["Measured Tput CW1"]) / 2
                    df["pmi"] = pd.Series(PMI_t)
                    pplogger.info(f"Updating values in csv from :{file_path}")
                    file = directory_path + '\\' + file_name+'.csv'
                    if os.path.isfile(file):
                        with open(file, 'a', newline = '') as f:
                            df.to_csv(f, header = False, index = False)
                    else:
                        df.to_csv(file, index = False)

                    cells, file_path = [], []
                    RI_T = []
                    CRNTI = []
                    cqi_rank1 = []
                    cqi_rank2_cw0 = []
                    cqi_rank2_cw1 = []
                    cqi_rank3_cw0 = []
                    cqi_rank3_cw1 = []
                    cqi_rank4_cw0 = []
                    cqi_rank4_cw1 = []
                    Rank1TxBLERCW0 = []
                    Rank2TxBLERCW0 = []
                    Rank2TxBLERCW1 = []
                    Rank3TxBLERCW0 = []
                    Rank3TxBLERCW1 = []
                    Rank4TxBLERCW0 = []
                    Rank4TxBLERCW1 = []
                    pdsch_failure = []
                    mcs_rank1 = []
                    mcs_rank2_cw0 = []
                    mcs_rank2_cw1 = []
                    mcs_rank3_cw0 = []
                    mcs_rank3_cw1 = []
                    mcs_rank4_cw0 = []
                    mcs_rank4_cw1 = []
                    s_cw0, s_cw1 = [], []
                    m_cw0, m_cw1 = [], []
                    PMI_t = []
        pplogger.info(f"Downlink mac stat's  values sucessfully updated in csv ")
        print("Im here too")
    except Exception as e:
        pplogger.error(f"Exception occured in getdlmacstats: {e}")
        raise TBSLTEError(f"Exception occured in getdlmacstats: {e}")
