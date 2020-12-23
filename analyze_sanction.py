# -*- coding: utf-8 -*- 
import time
import sys
import os
import pathlib
import logging
from logging import handlers
import traceback
import gc

from logger import module_logging
from conf import config

from LGE_normalizer import normalize_customer, normalize_supplier, normalize_maker, normalize_GERP
from LGE_filter import filter_country_LGE, filter_string_customer, filter_string_GERP, filter_string_supplier, filter_string_maker
from LGE_splitter import data_splitter
from LGE_term_frequency import TF_customer, TF_supplier, TF_maker, TF_GERP

from SANC_normalizer import normalize_sanction
from SANC_filter import filter_country_sanction, filter_string_sanction, filter_string_sanction_GERP
from SANC_integrator import integrator_entity_sanction, integrator_rep_sanction, integrator_GERP_sanction
from SANC_term_frequency import TF_sanction, TF_sanction_GERP

from analyzer import analyze_customer_entity, analyze_customer_rep, analyze_supplier_entity, analyze_supplier_rep, analyze_maker, analyze_GERP
from excel import process_report


def mkdir_ifnot(path):
    if not(os.path.isdir(path)):
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)


def notify_error(filepath, notifyname, content):
    errorpath = os.path.join(filepath, notifyname)
    if not(os.path.isfile(errorpath)):
        f = open(errorpath, "w")
        f.write(content)
        f.close()
    else:
        f = open(errorpath, "a")
        f.write("\n\n"+content)
        f.close()

"""
 Refractoring purpose
 
 1. improve time complexity in analyze function
    -remove object creation in nested loop
    -use deque instead of list
 2. refactor code 
 3. redesign process

"""


if __name__ == '__main__':

    try:

        ''' 프로그램 설정데이터 조회 '''
        CONFIG_DATA = config.CONFIG()

        ''' bat 파일로부터 수신한 인자 설정데이터 입력 '''
        print('Batch args : ', sys.argv)
        CONFIG_DATA.REPORT_DIR = sys.argv[1] + '\\' # 실행시점 보고서 디렉토리명
        CONFIG_DATA.PROCESS_CNT = sys.argv[2]       # 코어 숫자
        CONFIG_DATA.PROCESS_ORDER = sys.argv[3]     # 프로세스 순서
    
        ''' 로그파일 생성 디렉토리 설정 '''
        mkdir_ifnot(CONFIG_DATA.LOG_PATH)
        module_logging.setup_logger_notconsole('log_Analyze', r'./log/log_Analyze_' + CONFIG_DATA.start_time_path + '.txt')
        log_Analyze = module_logging.logging.getLogger('log_Analyze')
        log_Analyze.info('Analysis start')

        ''' 분석대상 선언 '''
        lgeData = ''    # 거래선
        sancData = ''   # 제재대상

        ''' 파일명 처리 '''
        filename = (CONFIG_DATA.TARGET_PATH.split('\\')[-1]).upper()
        print("filename ", filename)

        ''' 
            거래선 정규화
            국가코드 필터링
            문자열 빈도수 분석
            문자열 필터링
        '''
        if 'CUSTOMER' in filename:
            lgeData = normalize_customer.normalize( CONFIG_DATA )                           # 정규화 
            lgeData = filter_country_LGE.initiate( CONFIG_DATA, lgeData )                   # 국가코드 필터링
            
            TF_LGE_result = TF_customer.count( CONFIG_DATA, lgeData )                       # 국가별 문자열 빈도분석
            lgeData = filter_string_customer.filter( CONFIG_DATA, lgeData, TF_LGE_result )  # 국가별 문자열 필터링

        elif 'SUPPLIER' in filename:
            lgeData = normalize_supplier.normalize( CONFIG_DATA )                           # 정규화
            lgeData = filter_country_LGE.initiate( CONFIG_DATA, lgeData )                   # 국가코드 필터링
            
            TF_LGE_result = TF_supplier.count( CONFIG_DATA, lgeData )                       # 국가별 문자열 빈도분석
            lgeData = filter_string_supplier.filter( CONFIG_DATA, lgeData, TF_LGE_result )  # 국가별 문자열 필터링

        elif 'MAKER' in filename:
            lgeData = normalize_maker.normalize( CONFIG_DATA )                              # 정규화
            lgeData = filter_country_LGE.initiate( CONFIG_DATA, lgeData )                   # 국가코드 필터링
            
            TF_LGE_result = TF_maker.count( CONFIG_DATA, lgeData )                          # 국가별 문자열 빈도분석
            lgeData = filter_string_maker.filter( CONFIG_DATA, lgeData, TF_LGE_result )     # 국가별 문자열 필터링

        elif 'GERP' in filename:
            lgeData = normalize_GERP.normalize( CONFIG_DATA )                               # 정규화, GERP 국가코드 필터링 생략 (기본국가: MX)

            # TF_LGE_result = TF_GERP.count( CONFIG_DATA, lgeData )                           # 문자열 빈도분석
            # lgeData = filter_string_GERP.filter( CONFIG_DATA, lgeData, TF_LGE_result )      # 문자열 필터링

        else:
            raise ValueError('Unknown Source Filename, Source file must contain valid strings for their data type / [current name]:', filename)

        # ''' 거래선 데이터 분할 '''
        # lgeData = data_splitter.initiate( CONFIG_DATA, lgeData )                        # 거래선 CPU코어 숫자에 맞춰 분할

        # ''' 제재대상 정규화 '''
        sancData = normalize_sanction.normalize( CONFIG_DATA )

        ''' 
            검색형태/파일별 대상 제재대상 통합
            제재대상 TF 결과 문자열필터링
        '''
        searchType = CONFIG_DATA.SEARCH_TYPE                                                        # 검색형태 지정
        if 'GERP' in filename:
            sancData = integrator_GERP_sanction.filter( sancData )                                  # 거래선 'GERP' 일 시 전용 제재대상 통합모듈 / 국가코드 필터링 불필요

            TF_SANC_result = TF_sanction_GERP.count( CONFIG_DATA, sancData )                        # 문자열 빈도수 분석
            sancData = filter_string_sanction_GERP.filter( CONFIG_DATA, sancData, TF_SANC_result)   # 문자열 필터링

        elif 'MAKER' in filename:
            sancData = integrator_entity_sanction.filter( sancData )                                # 거래선 '공급' 일 시 Entity용 통합모듈
            sancData = filter_country_sanction.initiate( CONFIG_DATA, sancData )                    # 국가코드 필터링

            TF_SANC_result = TF_sanction.count( CONFIG_DATA, sancData )                             # 국가별 빈도수 분석
            sancData = filter_string_sanction.filter( CONFIG_DATA, sancData, TF_SANC_result )       # 국가별 문자열 필터링

        elif 'ENTITY' == searchType:
            sancData = integrator_entity_sanction.filter( sancData )                                # 제재대상 검색형태 업체명 통합
            sancData = filter_country_sanction.initiate( CONFIG_DATA, sancData )                    # 국가코드 필터링

            TF_SANC_result = TF_sanction.count( CONFIG_DATA, sancData )                             # 국가별 빈도수 분석
            sancData = filter_string_sanction.filter( CONFIG_DATA, sancData, TF_SANC_result )       # 국가별 문자열 필터링

        elif 'REP' == searchType:
            sancData = integrator_rep_sanction.filter( sancData )                                   #  제재대상 검색형태 개인명 통합
            sancData = filter_country_sanction.initiate( CONFIG_DATA, sancData )                    # 국가코드 필터링

            TF_SANC_result = TF_sanction.count( CONFIG_DATA, sancData )                             # 국가별 빈도수 분석
            sancData = filter_string_sanction.filter( CONFIG_DATA, sancData, TF_SANC_result )       # 국가별 문자열 필터링

        else:
            raise ValueError('Unknown Search type, value must be either of ENTITY or REP / current value : ', searchType)

        

        ''' 분석 실행  '''
        result = '' #분석결과 변수 선언

        if 'CUSTOMER' in filename:
            if searchType == 'ENTITY':
                result = analyze_customer_entity.start( CONFIG_DATA, sancData, lgeData )
            elif searchType == 'REP':
                result = analyze_customer_rep.start( CONFIG_DATA, sancData, lgeData )
            else:
                raise ValueError('No target searchType for Analysis')

        elif 'SUPPLIER' in filename:
            if searchType == 'ENTITY':
                result = analyze_supplier_entity.start( CONFIG_DATA, sancData, lgeData )
            elif searchType == 'REP':
                result = analyze_supplier_rep.start( CONFIG_DATA, sancData, lgeData )
            else:
                raise ValueError('No target searchType for Analysis')

        elif 'MAKER' in filename:
            result = analyze_maker.start( CONFIG_DATA, sancData, lgeData )
        elif 'GERP' in filename:
            result = analyze_GERP.start( CONFIG_DATA, sancData, lgeData )
            pass
        else:
            raise ValueError('No valid target name for Analysis')


        ''' generate excel report '''
        # process_report.create(CONFIG_DATA, result)

        ''' logging '''
        # log_Analyze.info('Analysis total elapsed time  : ' + str(time.time() - CONFIG_DATA.start_time) + ' seconds')
        # notify_error(CONFIG_DATA.OUTPUT_PATH, "finish.txt", "process complete")
        # log_Analyze.handlers[0].stream.close()
        # log_Analyze.removeHandler(log_Analyze.handlers[0])


    except Exception as e:
        print( 'Exception : ', str(e))
        eMsg = '\n' + str(traceback.format_exc())
        log_Analyze.error(eMsg)
        notify_error(CONFIG_DATA.OUTPUT_PATH, "error.txt", eMsg)
        log_Analyze.handlers[0].stream.close()
        log_Analyze.removeHandler(log_Analyze.handlers[0])
