; Create a clean network for AM and PM peak periods,
; reporting time period and peak hour volumes with intuitive field names.
; Also calculate V/C. Output as net/csv/dbf/shp.

; Usage: copy this file to the dir containing the loaded network .net file,
; set environmental variables, then run with `runtpp net_convert.s`
; TP: EA/AM/MD/PM/EV
; OUT_FORMAT: csv/dbf/shp
;             (.net is always outputed since it's another type of
;              control statement (NETO instead of LINKO))

; TODO take CUBENET_DIR as an envvar input, with default = pwd
; TODO V/C ratio output/calculations should be an option?
; TODO allow output of V1_1 to V18_1 as an option

IF ('%TP%' = 'AM')
  HR_FACTOR = 0.348  ; peak 1-hour factor
  TP_DUR = 3
  LANE = 'LANE_AM'
ELSEIF ('%TP%' = 'PM') 
  HR_FACTOR = 0.337  ; peak 1-hour factor
  TP_DUR = 3
  LANE = 'LANE_PM'
ELSEIF ('%TP%' = 'MD') 
  HR_FACTOR = 0.154  ; peak 1-hour factor
  TP_DUR = 6.5
  LANE = 'LANE_OP'
ELSEIF ('%TP%' = 'EV') 
  HR_FACTOR = 0.173  ; peak 1-hour factor
  TP_DUR = 8.5
  LANE = 'LANE_OP'
ELSEIF ('%TP%' = 'EA') 
  HR_FACTOR = 0.463  ; peak 1-hour factor
  TP_DUR = 3
  LANE = 'LANE_OP'
ENDIF

; might as well as output CSV since DBF is larger and more unwieldy
IF ('%OUT_FORMAT%' = 'dbf')
  LINKO_FORMAT = 'DBF'
  LINKO_FILE_EXT = 'dbf'
ELSEIF ('%OUT_FORMAT%' = 'shp')
  LINKO_FORMAT = 'SHP'  ; still outputs DBF, not shp
  LINKO_FILE_EXT = 'dbf'
ELSE  ; ELSEIF ('%OUT_FORMAT%' = 'csv')  ; just do CSV by default
  LINKO_FORMAT = 'CS1'
  LINKO_FILE_EXT = 'csv'
ENDIF

; TODO is there a way to have the column names in INCLUDE= in a separate var?

RUN PGM=NETWORK
    LINKI[1] = LOAD%TP%_FINAL.NET
    NETO = LOAD%TP%_FINAL_converted.net,
      INCLUDE=A,B,AT,FT,USE,CAP,STREETNAME,TYPE,MTYPE,DISTANCE,TOLL,
              LANE_AM,LANE_PM,LANE_OP,
              FF_SPD,FF_TIME,LOAD_SPD,LOAD_TIME,
              BUSLANE_AM,BUSLANE_PM,BUSLANE_OP,
              TOLLAM_DA,TOLLAM_SR2,TOLLAM_SR3,
              TOLLPM_DA,TOLLPM_SR2,TOLLPM_SR3,
              TOLLEA_DA,TOLLEA_SR2,TOLLEA_SR3,
              TOLLMD_DA,TOLLMD_SR2,TOLLMD_SR3,
              TOLLEV_DA,TOLLEV_SR2,TOLLEV_SR3,
              VALUETOLL_FLAG,PASSTHRU,BIKE_CLASS,PER_RISE,ONEWAY,
              DA,SR2,SR3,COM,TRK,BUS,TNC,AUTOVOL,PAXVOL,TOTVOL,PCEVOL,
              DA_1HR,SR2_1HR,SR3_1HR,COM_1HR,TRK_1HR,BUS_1HR,TNC_1HR,
              AUTOVOL_1HR,PAXVOL_1HR,TOTVOL_1HR,PCEVOL_1HR,VC_RATIO
    LINKO = LOAD%TP%_FINAL_converted.@LINKO_FILE_EXT@,FORMAT=@LINKO_FORMAT@,
      INCLUDE=A,B,AT,FT,USE,CAP,STREETNAME,TYPE,MTYPE,DISTANCE,TOLL,
              LANE_AM,LANE_PM,LANE_OP,
              FF_SPD,FF_TIME,LOAD_SPD,LOAD_TIME,
              BUSLANE_AM,BUSLANE_PM,BUSLANE_OP,
              TOLLAM_DA,TOLLAM_SR2,TOLLAM_SR3,
              TOLLPM_DA,TOLLPM_SR2,TOLLPM_SR3,
              TOLLEA_DA,TOLLEA_SR2,TOLLEA_SR3,
              TOLLMD_DA,TOLLMD_SR2,TOLLMD_SR3,
              TOLLEV_DA,TOLLEV_SR2,TOLLEV_SR3,
              VALUETOLL_FLAG,PASSTHRU,BIKE_CLASS,PER_RISE,ONEWAY,
              DA,SR2,SR3,COM,TRK,BUS,TNC,AUTOVOL,PAXVOL,TOTVOL,PCEVOL,
              DA_1HR,SR2_1HR,SR3_1HR,COM_1HR,TRK_1HR,BUS_1HR,TNC_1HR,
              AUTOVOL_1HR,PAXVOL_1HR,TOTVOL_1HR,PCEVOL_1HR,VC_RATIO

    ; FT 6 is for both centroid connectors (which are coded to have 7 links)
    ; and express lane access/egress virtual links.
    ; `FT==6 && LANE_%TP%==7` is equivalent to `A <= 4000 || B <= 4000`
    ; IF (FT==6) DELETE  ; to also delete the express lane access links
    IF (FT==6 && @LANE@==7) DELETE  ; to only delete centroid connectors
    FF_SPD   =SPEED
    FF_TIME  =TIME
    LOAD_SPD =CSPD_1
    LOAD_TIME=TIME_1
    
    DA      = V1_1  + V4_1  + V7_1    ; drive alone
    SR2     = V2_1  + V5_1  + V8_1    ; shared ride 2 people
    SR3     = V3_1  + V6_1  + V9_1    ; shared ride 3+ people
    TRK     = V10_1 + V11_1 + V12_1   ; truck
    COM     = V13_1 + V14_1 + V15_1   ; commercial vehicles
    TNC     = V16_1 + V17_1 + V18_1   ; TNC   
    BUS     = BUSVOL_%TP%             ; bus
    AUTOVOL = DA + SR2 + SR3          ; total no. of automobiles
    PAXVOL  = DA + 2*SR2 + 3.5*SR3    ; total no. of people in autos (assuming
                                      ; 3.5 people on average in each SR3) 
    TOTVOL  = AUTOVOL + TRK + COM + TNC + BUS  ; total no. of vehicles
    PCEVOL  = AUTOVOL + 2*TRK + COM + TNC + 2*BUS  ; passenger car equivalent
    
    ; peak 1-hour volumes
    DA_1HR  = DA  * @HR_FACTOR@
    SR2_1HR = SR2 * @HR_FACTOR@
    SR3_1HR = SR3 * @HR_FACTOR@
    TRK_1HR = TRK * @HR_FACTOR@
    COM_1HR = COM * @HR_FACTOR@
    TNC_1HR = TNC * @HR_FACTOR@
    BUS_1HR = BUS * (1/@TP_DUR@)
    AUTOVOL_1HR = AUTOVOL * @HR_FACTOR@
    PAXVOL_1HR  = PAXVOL  * @HR_FACTOR@
    TOTVOL_1HR  = TOTVOL  * @HR_FACTOR@
    PCEVOL_1HR  = PCEVOL  * @HR_FACTOR@

    VC_RATIO = 0
    IF (@LANE@>0)
      VC_RATIO = PCEVOL_1HR / (CAP * @LANE@)
    ENDIF
ENDRUN