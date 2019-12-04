-- Manual pivot example DDL Script
--
-- This script demonstrates how to build a pivot table manually. While it is can be an effective method for building pivots, for larger 
-- sets of data with, for example, large and varied numbers of features for each object, it is demonstrably unwieldy and is included here
-- only for reference.
--
-- NOTE: Some data platforms have a PIVOT functionality incorporated in the version of SQL, (MS SQL Server is an example)
-- this is not a universal standard.
-- If natural PIVOT functionality is abailable, refer to the documentation for that particular platform.
--
-- first we find the maximum number of entries for any single object in the data set
select max(mjdct) from
(
    select object_id,count(mjd) as mjdct
    from training_set
    group by object_id
) a

-- now we build the query values for the pivot. this is an example of using SQL to build SQL.
-- This query will output the pivoted fields as a max function in a group by statement; which is then used to build the query
with cnt
as
(
    select a.x from
    (
        select row_number() over () as x
        from training_set
        
    ) a
    where a.x < 353
)
select concat(",max(case when rownum=" , cnt.x , " then mjd end) mjd", cnt.x )  stmnt from cnt    

-- Now we run the query. Note that this query generates the MJD pivot. To create the other features, you simple replace the
-- MJD column with the column of the feature required. You will also need to change the table name to reflect the data; ie for
-- a flux pivot, the table name would be similar to "full_flux_pivot" and you would be selecting the flux values, not mjd.
create table if not exists full_mjd_pivot
stored as parquet
as
with mjddata as 
(
    select 
        object_id,mjd,
        row_number() over (partition by object_id order by mjd) as rowNum
    from training_set 

)
select object_id
-- Lines below demonstrate the output of the query we used above 
,max(case when rownum=1 then mjd end) mjd1
,max(case when rownum=2 then mjd end) mjd2
,max(case when rownum=3 then mjd end) mjd3
,max(case when rownum=4 then mjd end) mjd4
,max(case when rownum=5 then mjd end) mjd5
,max(case when rownum=6 then mjd end) mjd6
,max(case when rownum=7 then mjd end) mjd7
,max(case when rownum=8 then mjd end) mjd8
,max(case when rownum=9 then mjd end) mjd9
,max(case when rownum=10 then mjd end) mjd10
,max(case when rownum=11 then mjd end) mjd11
,max(case when rownum=12 then mjd end) mjd12
,max(case when rownum=13 then mjd end) mjd13
,max(case when rownum=14 then mjd end) mjd14
,max(case when rownum=15 then mjd end) mjd15
,max(case when rownum=16 then mjd end) mjd16
,max(case when rownum=17 then mjd end) mjd17
,max(case when rownum=18 then mjd end) mjd18
,max(case when rownum=19 then mjd end) mjd19
,max(case when rownum=20 then mjd end) mjd20
,max(case when rownum=21 then mjd end) mjd21
,max(case when rownum=22 then mjd end) mjd22
,max(case when rownum=23 then mjd end) mjd23
,max(case when rownum=24 then mjd end) mjd24
,max(case when rownum=25 then mjd end) mjd25
,max(case when rownum=26 then mjd end) mjd26
,max(case when rownum=27 then mjd end) mjd27
,max(case when rownum=28 then mjd end) mjd28
,max(case when rownum=29 then mjd end) mjd29
,max(case when rownum=30 then mjd end) mjd30
,max(case when rownum=31 then mjd end) mjd31
,max(case when rownum=32 then mjd end) mjd32
,max(case when rownum=33 then mjd end) mjd33
,max(case when rownum=34 then mjd end) mjd34
,max(case when rownum=35 then mjd end) mjd35
,max(case when rownum=36 then mjd end) mjd36
,max(case when rownum=37 then mjd end) mjd37
,max(case when rownum=38 then mjd end) mjd38
,max(case when rownum=39 then mjd end) mjd39
,max(case when rownum=40 then mjd end) mjd40
,max(case when rownum=41 then mjd end) mjd41
,max(case when rownum=42 then mjd end) mjd42
,max(case when rownum=43 then mjd end) mjd43
,max(case when rownum=44 then mjd end) mjd44
,max(case when rownum=45 then mjd end) mjd45
,max(case when rownum=46 then mjd end) mjd46
,max(case when rownum=47 then mjd end) mjd47
,max(case when rownum=48 then mjd end) mjd48
,max(case when rownum=49 then mjd end) mjd49
,max(case when rownum=50 then mjd end) mjd50
,max(case when rownum=51 then mjd end) mjd51
,max(case when rownum=52 then mjd end) mjd52
,max(case when rownum=53 then mjd end) mjd53
,max(case when rownum=54 then mjd end) mjd54
,max(case when rownum=55 then mjd end) mjd55
,max(case when rownum=56 then mjd end) mjd56
,max(case when rownum=57 then mjd end) mjd57
,max(case when rownum=58 then mjd end) mjd58
,max(case when rownum=59 then mjd end) mjd59
,max(case when rownum=60 then mjd end) mjd60
,max(case when rownum=61 then mjd end) mjd61
,max(case when rownum=62 then mjd end) mjd62
,max(case when rownum=63 then mjd end) mjd63
,max(case when rownum=64 then mjd end) mjd64
,max(case when rownum=65 then mjd end) mjd65
,max(case when rownum=66 then mjd end) mjd66
,max(case when rownum=67 then mjd end) mjd67
,max(case when rownum=68 then mjd end) mjd68
,max(case when rownum=69 then mjd end) mjd69
,max(case when rownum=70 then mjd end) mjd70
,max(case when rownum=71 then mjd end) mjd71
,max(case when rownum=72 then mjd end) mjd72
,max(case when rownum=73 then mjd end) mjd73
,max(case when rownum=74 then mjd end) mjd74
,max(case when rownum=75 then mjd end) mjd75
,max(case when rownum=76 then mjd end) mjd76
,max(case when rownum=77 then mjd end) mjd77
,max(case when rownum=78 then mjd end) mjd78
,max(case when rownum=79 then mjd end) mjd79
,max(case when rownum=80 then mjd end) mjd80
,max(case when rownum=81 then mjd end) mjd81
,max(case when rownum=82 then mjd end) mjd82
,max(case when rownum=83 then mjd end) mjd83
,max(case when rownum=84 then mjd end) mjd84
,max(case when rownum=85 then mjd end) mjd85
,max(case when rownum=86 then mjd end) mjd86
,max(case when rownum=87 then mjd end) mjd87
,max(case when rownum=88 then mjd end) mjd88
,max(case when rownum=89 then mjd end) mjd89
,max(case when rownum=90 then mjd end) mjd90
,max(case when rownum=91 then mjd end) mjd91
,max(case when rownum=92 then mjd end) mjd92
,max(case when rownum=93 then mjd end) mjd93
,max(case when rownum=94 then mjd end) mjd94
,max(case when rownum=95 then mjd end) mjd95
,max(case when rownum=96 then mjd end) mjd96
,max(case when rownum=97 then mjd end) mjd97
,max(case when rownum=98 then mjd end) mjd98
,max(case when rownum=99 then mjd end) mjd99
,max(case when rownum=100 then mjd end) mjd100
,max(case when rownum=101 then mjd end) mjd101
,max(case when rownum=102 then mjd end) mjd102
,max(case when rownum=103 then mjd end) mjd103
,max(case when rownum=104 then mjd end) mjd104
,max(case when rownum=105 then mjd end) mjd105
,max(case when rownum=106 then mjd end) mjd106
,max(case when rownum=107 then mjd end) mjd107
,max(case when rownum=108 then mjd end) mjd108
,max(case when rownum=109 then mjd end) mjd109
,max(case when rownum=110 then mjd end) mjd110
,max(case when rownum=111 then mjd end) mjd111
,max(case when rownum=112 then mjd end) mjd112
,max(case when rownum=113 then mjd end) mjd113
,max(case when rownum=114 then mjd end) mjd114
,max(case when rownum=115 then mjd end) mjd115
,max(case when rownum=116 then mjd end) mjd116
,max(case when rownum=117 then mjd end) mjd117
,max(case when rownum=118 then mjd end) mjd118
,max(case when rownum=119 then mjd end) mjd119
,max(case when rownum=120 then mjd end) mjd120
,max(case when rownum=121 then mjd end) mjd121
,max(case when rownum=122 then mjd end) mjd122
,max(case when rownum=123 then mjd end) mjd123
,max(case when rownum=124 then mjd end) mjd124
,max(case when rownum=125 then mjd end) mjd125
,max(case when rownum=126 then mjd end) mjd126
,max(case when rownum=127 then mjd end) mjd127
,max(case when rownum=128 then mjd end) mjd128
,max(case when rownum=129 then mjd end) mjd129
,max(case when rownum=130 then mjd end) mjd130
,max(case when rownum=131 then mjd end) mjd131
,max(case when rownum=132 then mjd end) mjd132
,max(case when rownum=133 then mjd end) mjd133
,max(case when rownum=134 then mjd end) mjd134
,max(case when rownum=135 then mjd end) mjd135
,max(case when rownum=136 then mjd end) mjd136
,max(case when rownum=137 then mjd end) mjd137
,max(case when rownum=138 then mjd end) mjd138
,max(case when rownum=139 then mjd end) mjd139
,max(case when rownum=140 then mjd end) mjd140
,max(case when rownum=141 then mjd end) mjd141
,max(case when rownum=142 then mjd end) mjd142
,max(case when rownum=143 then mjd end) mjd143
,max(case when rownum=144 then mjd end) mjd144
,max(case when rownum=145 then mjd end) mjd145
,max(case when rownum=146 then mjd end) mjd146
,max(case when rownum=147 then mjd end) mjd147
,max(case when rownum=148 then mjd end) mjd148
,max(case when rownum=149 then mjd end) mjd149
,max(case when rownum=150 then mjd end) mjd150
,max(case when rownum=151 then mjd end) mjd151
,max(case when rownum=152 then mjd end) mjd152
,max(case when rownum=153 then mjd end) mjd153
,max(case when rownum=154 then mjd end) mjd154
,max(case when rownum=155 then mjd end) mjd155
,max(case when rownum=156 then mjd end) mjd156
,max(case when rownum=157 then mjd end) mjd157
,max(case when rownum=158 then mjd end) mjd158
,max(case when rownum=159 then mjd end) mjd159
,max(case when rownum=160 then mjd end) mjd160
,max(case when rownum=161 then mjd end) mjd161
,max(case when rownum=162 then mjd end) mjd162
,max(case when rownum=163 then mjd end) mjd163
,max(case when rownum=164 then mjd end) mjd164
,max(case when rownum=165 then mjd end) mjd165
,max(case when rownum=166 then mjd end) mjd166
,max(case when rownum=167 then mjd end) mjd167
,max(case when rownum=168 then mjd end) mjd168
,max(case when rownum=169 then mjd end) mjd169
,max(case when rownum=170 then mjd end) mjd170
,max(case when rownum=171 then mjd end) mjd171
,max(case when rownum=172 then mjd end) mjd172
,max(case when rownum=173 then mjd end) mjd173
,max(case when rownum=174 then mjd end) mjd174
,max(case when rownum=175 then mjd end) mjd175
,max(case when rownum=176 then mjd end) mjd176
,max(case when rownum=177 then mjd end) mjd177
,max(case when rownum=178 then mjd end) mjd178
,max(case when rownum=179 then mjd end) mjd179
,max(case when rownum=180 then mjd end) mjd180
,max(case when rownum=181 then mjd end) mjd181
,max(case when rownum=182 then mjd end) mjd182
,max(case when rownum=183 then mjd end) mjd183
,max(case when rownum=184 then mjd end) mjd184
,max(case when rownum=185 then mjd end) mjd185
,max(case when rownum=186 then mjd end) mjd186
,max(case when rownum=187 then mjd end) mjd187
,max(case when rownum=188 then mjd end) mjd188
,max(case when rownum=189 then mjd end) mjd189
,max(case when rownum=190 then mjd end) mjd190
,max(case when rownum=191 then mjd end) mjd191
,max(case when rownum=192 then mjd end) mjd192
,max(case when rownum=193 then mjd end) mjd193
,max(case when rownum=194 then mjd end) mjd194
,max(case when rownum=195 then mjd end) mjd195
,max(case when rownum=196 then mjd end) mjd196
,max(case when rownum=197 then mjd end) mjd197
,max(case when rownum=198 then mjd end) mjd198
,max(case when rownum=199 then mjd end) mjd199
,max(case when rownum=200 then mjd end) mjd200
,max(case when rownum=201 then mjd end) mjd201
,max(case when rownum=202 then mjd end) mjd202
,max(case when rownum=203 then mjd end) mjd203
,max(case when rownum=204 then mjd end) mjd204
,max(case when rownum=205 then mjd end) mjd205
,max(case when rownum=206 then mjd end) mjd206
,max(case when rownum=207 then mjd end) mjd207
,max(case when rownum=208 then mjd end) mjd208
,max(case when rownum=209 then mjd end) mjd209
,max(case when rownum=210 then mjd end) mjd210
,max(case when rownum=211 then mjd end) mjd211
,max(case when rownum=212 then mjd end) mjd212
,max(case when rownum=213 then mjd end) mjd213
,max(case when rownum=214 then mjd end) mjd214
,max(case when rownum=215 then mjd end) mjd215
,max(case when rownum=216 then mjd end) mjd216
,max(case when rownum=217 then mjd end) mjd217
,max(case when rownum=218 then mjd end) mjd218
,max(case when rownum=219 then mjd end) mjd219
,max(case when rownum=220 then mjd end) mjd220
,max(case when rownum=221 then mjd end) mjd221
,max(case when rownum=222 then mjd end) mjd222
,max(case when rownum=223 then mjd end) mjd223
,max(case when rownum=224 then mjd end) mjd224
,max(case when rownum=225 then mjd end) mjd225
,max(case when rownum=226 then mjd end) mjd226
,max(case when rownum=227 then mjd end) mjd227
,max(case when rownum=228 then mjd end) mjd228
,max(case when rownum=229 then mjd end) mjd229
,max(case when rownum=230 then mjd end) mjd230
,max(case when rownum=231 then mjd end) mjd231
,max(case when rownum=232 then mjd end) mjd232
,max(case when rownum=233 then mjd end) mjd233
,max(case when rownum=234 then mjd end) mjd234
,max(case when rownum=235 then mjd end) mjd235
,max(case when rownum=236 then mjd end) mjd236
,max(case when rownum=237 then mjd end) mjd237
,max(case when rownum=238 then mjd end) mjd238
,max(case when rownum=239 then mjd end) mjd239
,max(case when rownum=240 then mjd end) mjd240
,max(case when rownum=241 then mjd end) mjd241
,max(case when rownum=242 then mjd end) mjd242
,max(case when rownum=243 then mjd end) mjd243
,max(case when rownum=244 then mjd end) mjd244
,max(case when rownum=245 then mjd end) mjd245
,max(case when rownum=246 then mjd end) mjd246
,max(case when rownum=247 then mjd end) mjd247
,max(case when rownum=248 then mjd end) mjd248
,max(case when rownum=249 then mjd end) mjd249
,max(case when rownum=250 then mjd end) mjd250
,max(case when rownum=251 then mjd end) mjd251
,max(case when rownum=252 then mjd end) mjd252
,max(case when rownum=253 then mjd end) mjd253
,max(case when rownum=254 then mjd end) mjd254
,max(case when rownum=255 then mjd end) mjd255
,max(case when rownum=256 then mjd end) mjd256
,max(case when rownum=257 then mjd end) mjd257
,max(case when rownum=258 then mjd end) mjd258
,max(case when rownum=259 then mjd end) mjd259
,max(case when rownum=260 then mjd end) mjd260
,max(case when rownum=261 then mjd end) mjd261
,max(case when rownum=262 then mjd end) mjd262
,max(case when rownum=263 then mjd end) mjd263
,max(case when rownum=264 then mjd end) mjd264
,max(case when rownum=265 then mjd end) mjd265
,max(case when rownum=266 then mjd end) mjd266
,max(case when rownum=267 then mjd end) mjd267
,max(case when rownum=268 then mjd end) mjd268
,max(case when rownum=269 then mjd end) mjd269
,max(case when rownum=270 then mjd end) mjd270
,max(case when rownum=271 then mjd end) mjd271
,max(case when rownum=272 then mjd end) mjd272
,max(case when rownum=273 then mjd end) mjd273
,max(case when rownum=274 then mjd end) mjd274
,max(case when rownum=275 then mjd end) mjd275
,max(case when rownum=276 then mjd end) mjd276
,max(case when rownum=277 then mjd end) mjd277
,max(case when rownum=278 then mjd end) mjd278
,max(case when rownum=279 then mjd end) mjd279
,max(case when rownum=280 then mjd end) mjd280
,max(case when rownum=281 then mjd end) mjd281
,max(case when rownum=282 then mjd end) mjd282
,max(case when rownum=283 then mjd end) mjd283
,max(case when rownum=284 then mjd end) mjd284
,max(case when rownum=285 then mjd end) mjd285
,max(case when rownum=286 then mjd end) mjd286
,max(case when rownum=287 then mjd end) mjd287
,max(case when rownum=288 then mjd end) mjd288
,max(case when rownum=289 then mjd end) mjd289
,max(case when rownum=290 then mjd end) mjd290
,max(case when rownum=291 then mjd end) mjd291
,max(case when rownum=292 then mjd end) mjd292
,max(case when rownum=293 then mjd end) mjd293
,max(case when rownum=294 then mjd end) mjd294
,max(case when rownum=295 then mjd end) mjd295
,max(case when rownum=296 then mjd end) mjd296
,max(case when rownum=297 then mjd end) mjd297
,max(case when rownum=298 then mjd end) mjd298
,max(case when rownum=299 then mjd end) mjd299
,max(case when rownum=300 then mjd end) mjd300
,max(case when rownum=301 then mjd end) mjd301
,max(case when rownum=302 then mjd end) mjd302
,max(case when rownum=303 then mjd end) mjd303
,max(case when rownum=304 then mjd end) mjd304
,max(case when rownum=305 then mjd end) mjd305
,max(case when rownum=306 then mjd end) mjd306
,max(case when rownum=307 then mjd end) mjd307
,max(case when rownum=308 then mjd end) mjd308
,max(case when rownum=309 then mjd end) mjd309
,max(case when rownum=310 then mjd end) mjd310
,max(case when rownum=311 then mjd end) mjd311
,max(case when rownum=312 then mjd end) mjd312
,max(case when rownum=313 then mjd end) mjd313
,max(case when rownum=314 then mjd end) mjd314
,max(case when rownum=315 then mjd end) mjd315
,max(case when rownum=316 then mjd end) mjd316
,max(case when rownum=317 then mjd end) mjd317
,max(case when rownum=318 then mjd end) mjd318
,max(case when rownum=319 then mjd end) mjd319
,max(case when rownum=320 then mjd end) mjd320
,max(case when rownum=321 then mjd end) mjd321
,max(case when rownum=322 then mjd end) mjd322
,max(case when rownum=323 then mjd end) mjd323
,max(case when rownum=324 then mjd end) mjd324
,max(case when rownum=325 then mjd end) mjd325
,max(case when rownum=326 then mjd end) mjd326
,max(case when rownum=327 then mjd end) mjd327
,max(case when rownum=328 then mjd end) mjd328
,max(case when rownum=329 then mjd end) mjd329
,max(case when rownum=330 then mjd end) mjd330
,max(case when rownum=331 then mjd end) mjd331
,max(case when rownum=332 then mjd end) mjd332
,max(case when rownum=333 then mjd end) mjd333
,max(case when rownum=334 then mjd end) mjd334
,max(case when rownum=335 then mjd end) mjd335
,max(case when rownum=336 then mjd end) mjd336
,max(case when rownum=337 then mjd end) mjd337
,max(case when rownum=338 then mjd end) mjd338
,max(case when rownum=339 then mjd end) mjd339
,max(case when rownum=340 then mjd end) mjd340
,max(case when rownum=341 then mjd end) mjd341
,max(case when rownum=342 then mjd end) mjd342
,max(case when rownum=343 then mjd end) mjd343
,max(case when rownum=344 then mjd end) mjd344
,max(case when rownum=345 then mjd end) mjd345
,max(case when rownum=346 then mjd end) mjd346
,max(case when rownum=347 then mjd end) mjd347
,max(case when rownum=348 then mjd end) mjd348
,max(case when rownum=349 then mjd end) mjd349
,max(case when rownum=350 then mjd end) mjd350
,max(case when rownum=351 then mjd end) mjd351
,max(case when rownum=352 then mjd end) mjd352
from mjddata
group by object_id
