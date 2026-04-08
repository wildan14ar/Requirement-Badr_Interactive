# main.py
import argparse
import sys
import os

# Add scripts directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from pyspark.sql import SparkSession
from helpers import build_jdbc_url, jdbc_read
from methods import run_method


ETL_JOBS = [
    {
        "name": "surat_eksternal",
        "sql": "SELECT id, judul FROM adele.surat_eksternal",
        "target_table": "public.surat_eksternal",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_internal",
        "sql": "SELECT id, kode, judul, keterangan, kode_surat, cby, cdt FROM adele.surat_internal",
        "target_table": "public.surat_internal",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_kategori",
        "sql": "SELECT id, kode, judul, tipe FROM adele.surat_kategori",
        "target_table": "public.surat_kategori",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_keluar_memo",
        "sql": 'SELECT id, documentid, id_masuk_chained, "user", show, wfdone, opened FROM adele.surat_keluar_memo',
        "target_table": "public.surat_keluar_memo",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_masuk_chained",
        "sql": 'SELECT id, id_surat_keluar, dibaca, dibaca_oleh, date, dept_surat_masuk, "user", folder, folderarsip, wfdone, todept, tampil FROM adele.surat_masuk_chained',
        "target_table": "public.surat_masuk_chained",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_masuk_chained_gm",
        "sql": 'SELECT id, documentid, id_masuk_chained, "user", show, wfdone FROM adele.surat_masuk_chained_gm',
        "target_table": "public.surat_masuk_chained_gm",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_masuk_disposisi",
        "sql": "SELECT id, id_surat_masuk, added, userid, keterangan, dibaca, dibaca_oleh, pengirim, balasan, dibaca_date, dibalas_date, tampil FROM adele.surat_masuk_disposisi",
        "target_table": "public.surat_masuk_disposisi",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_masuk_notif",
        "sql": 'SELECT id, surat_masuk_id, "user", added, tampil, has_file FROM adele.surat_masuk_notif',
        "target_table": "public.surat_masuk_notif",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "surat_sifat",
        "sql": "SELECT id, judul FROM adele.surat_sifat",
        "target_table": "public.surat_sifat",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblacls",
        "sql": "SELECT id, target, targettype, userid, groupid, mode, is_share, exp FROM adele.tblacls",
        "target_table": "public.tblacls",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbladditionalrelated",
        "sql": "SELECT addid, documentid, related, level, type, template, date, owner, addkey, locked FROM adele.tbladditionalrelated",
        "target_table": "public.tbladditionalrelated",
        "conflict_columns": ["addid"],
        "method": "upsert",
    },
    {
        "name": "tblconfig",
        "sql": "SELECT companyname, country, province, city, postal, street, phone, fax, email, smtp_host, smtp_user, smtp_password, smtp_port, smtp_ssl, smtp_debug, smtp_sender, maxduration, maxcookie, ocr_show, addenable FROM adele.tblconfig",
        "target_table": "public.tblconfig",
        "conflict_columns": [],
        "method": "upsert",
    },
    {
        "name": "tbldocumentapprovelog",
        "sql": "SELECT approvelogid, approveid, status, comment, date, userid, show FROM adele.tbldocumentapprovelog",
        "target_table": "public.tbldocumentapprovelog",
        "conflict_columns": ["approvelogid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentapprovers",
        "sql": "SELECT approveid, documentid, version, type, required, exp, userdeadline, grpdeadline, sendemail, hash FROM adele.tbldocumentapprovers",
        "target_table": "public.tbldocumentapprovers",
        "conflict_columns": ["approveid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentapproversserial",
        "sql": "SELECT approveid, documentid, version, type, required, exp, userdeadline, grpdeadline FROM adele.tbldocumentapproversserial",
        "target_table": "public.tbldocumentapproversserial",
        "conflict_columns": ["approveid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentcontent",
        "sql": "SELECT document, version, revisi, comment, date, createdby, dir, orgfilename, filetype, mimetype, workflow, sourcefrom, eo, parsel, skact, revised FROM adele.tbldocumentcontent",
        "target_table": "public.tbldocumentcontent",
        "conflict_columns": ["document", "version"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentencript",
        "sql": "SELECT document, userid FROM adele.tbldocumentencript",
        "target_table": "public.tbldocumentencript",
        "conflict_columns": ["document"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentfiles",
        "sql": "SELECT id, document, userid, comment, name, date, dir, orgfilename, filetype, mimetype FROM adele.tbldocumentfiles",
        "target_table": "public.tbldocumentfiles",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentlinks",
        "sql": "SELECT id, document, target, userid, public FROM adele.tbldocumentlinks",
        "target_table": "public.tbldocumentlinks",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentlocks",
        "sql": "SELECT document, userid FROM adele.tbldocumentlocks",
        "target_table": "public.tbldocumentlocks",
        "conflict_columns": ["document"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentpo",
        "sql": "SELECT poid, ponumber, userlist, grplist FROM adele.tbldocumentpo",
        "target_table": "public.tbldocumentpo",
        "conflict_columns": ["poid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentreviewers",
        "sql": "SELECT reviewid, documentid, version, type, required, exp, sendemail, hash, wftype, isdrafter FROM adele.tbldocumentreviewers",
        "target_table": "public.tbldocumentreviewers",
        "conflict_columns": ["reviewid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentreviewersserial",
        "sql": "SELECT reviewid, documentid, version, type, required, exp, wftype, is_removed FROM adele.tbldocumentreviewersserial",
        "target_table": "public.tbldocumentreviewersserial",
        "conflict_columns": ["reviewid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentreviewlog",
        "sql": "SELECT reviewlogid, reviewid, status, comment, date, userid, show FROM adele.tbldocumentreviewlog",
        "target_table": "public.tbldocumentreviewlog",
        "conflict_columns": ["reviewlogid"],
        "method": "upsert",
    },
    {
        "name": "tbldocuments",
        "sql": """SELECT id, name, comment, date, rem1, rem2, expires, owner, folder, folderlist, inheritaccess, defaultaccess, locked, keywords, sequence, genecode, formbuilder, formid, wftype, sendexpiredstatus, borrowstatus, borrowby, borrowreturn, borrowreason, downloadable, pendingdelete, autorelated, project, doc_yr, doc_mo, seq_no, doc_no, vo, kind, keluar_nosurat, keluar_tipe, keluar_penerima, keluar_dept_from, keluar_bulan, keluar_tahun, keluar_core_kategori, keluar_core_kode_surat_divisi, keluar_core_pembuat_surat, keluar_core_penerima, keluar_core_jabatan, keluar_core_sifat_surat, keluar_core_lampiran, keluar_core_perihal, act_urgensi, masuk_instansi, masuk_nama_pengirim, masuk_kategori, masuk_unit_penerima, masuk_nama_penerima, masuk_sifat, masuk_nomor, masuk_tanggal, masuk_ret1, masuk_ret2, masuk_tanggal_terima, masuk_via, masuk_resi, masuk_terima_note, masuk_jlampiran, masuk_nama_penerima_surat, masuk_read_by, keluar_kirim_status, keluar_kirim_by, keluar_kirim_date, keluar_kirim_note, keluar_booking, act_pesan_drafter, keluar_nosurat_plain, keluar_nosurat_plain_edited, masuk_nama_penerima_user, read_by_staff, keluar_kirim_expedition_file, keluar_kirim_expedition_file_type, terkirim, spub, terkirim_ak, terkirim_dtime FROM adele.tbldocuments""",
        "target_table": "public.tbldocuments",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentstatus",
        "sql": "SELECT statusid, documentid, version FROM adele.tbldocumentstatus",
        "target_table": "public.tbldocumentstatus",
        "conflict_columns": ["statusid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumentstatuslog",
        "sql": "SELECT statuslogid, statusid, status, comment, date, userid, show FROM adele.tbldocumentstatuslog",
        "target_table": "public.tbldocumentstatuslog",
        "conflict_columns": ["statuslogid"],
        "method": "upsert",
    },
    {
        "name": "tbldocumenttemplate",
        "sql": "SELECT templateid, templatename, templatefile, templategroup, lembar, html FROM adele.tbldocumenttemplate",
        "target_table": "public.tbldocumenttemplate",
        "conflict_columns": ["templateid"],
        "method": "upsert",
    },
    {
        "name": "tblfolders",
        "sql": "SELECT id, name, parent, comment, owner, inheritaccess, defaultaccess, sequence, code FROM adele.tblfolders",
        "target_table": "public.tblfolders",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblforum",
        "sql": "SELECT forumid, documentid, userid, comment, date FROM adele.tblforum",
        "target_table": "public.tblforum",
        "conflict_columns": ["forumid"],
        "method": "upsert",
    },
    {
        "name": "tblforumlog",
        "sql": "SELECT forumlogid, forumid, userid, status, date FROM adele.tblforumlog",
        "target_table": "public.tblforumlog",
        "conflict_columns": ["forumlogid"],
        "method": "upsert",
    },
    {
        "name": "tblgroupmembers",
        "sql": "SELECT groupid, userid, manager FROM adele.tblgroupmembers",
        "target_table": "public.tblgroupmembers",
        "conflict_columns": ["groupid", "userid"],
        "method": "upsert",
    },
    {
        "name": "tblgroups",
        "sql": "SELECT id, name, comment, kode, sekber, ap, divisi FROM adele.tblgroups",
        "target_table": "public.tblgroups",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblkeluartembusan",
        "sql": "SELECT id, document, version, usertembusan, added FROM adele.tblkeluartembusan",
        "target_table": "public.tblkeluartembusan",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblkeywordcategories",
        "sql": "SELECT id, name, owner FROM adele.tblkeywordcategories",
        "target_table": "public.tblkeywordcategories",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblkeywords",
        "sql": "SELECT id, category, keywords FROM adele.tblkeywords",
        "target_table": "public.tblkeywords",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblmandatoryapprovers",
        "sql": "SELECT userid, approveruserid, approvergroupid FROM adele.tblmandatoryapprovers",
        "target_table": "public.tblmandatoryapprovers",
        "conflict_columns": ["userid", "approveruserid", "approvergroupid"],
        "method": "upsert",
    },
    {
        "name": "tblmandatoryreviewers",
        "sql": "SELECT userid, revieweruserid, reviewergroupid FROM adele.tblmandatoryreviewers",
        "target_table": "public.tblmandatoryreviewers",
        "conflict_columns": ["userid", "revieweruserid", "reviewergroupid"],
        "method": "upsert",
    },
    {
        "name": "tblnotify",
        "sql": "SELECT target, targettype, userid, groupid FROM adele.tblnotify",
        "target_table": "public.tblnotify",
        "conflict_columns": ["target", "targettype", "userid", "groupid"],
        "method": "upsert",
    },
    {
        "name": "tblsekdir",
        "sql": 'SELECT id, "user", sekdir FROM adele.tblsekdir',
        "target_table": "public.tblsekdir",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblsessions",
        "sql": "SELECT id, userid, lastaccess, autologout, theme, language, ip, os, browser FROM adele.tblsessions",
        "target_table": "public.tblsessions",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblsubemail",
        "sql": "SELECT id, value FROM adele.tblsubemail",
        "target_table": "public.tblsubemail",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblsuratmasukshadow",
        "sql": 'SELECT id, document, "user", status, subject, show FROM adele.tblsuratmasukshadow',
        "target_table": "public.tblsuratmasukshadow",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbltempkontinyu",
        "sql": "SELECT id, type, dept_code, last_letter_no, inserted FROM adele.tbltempkontinyu",
        "target_table": "public.tbltempkontinyu",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbltmprecyclebin",
        "sql": "SELECT idbin, objid, type, from_dir, when_delete FROM adele.tbltmprecyclebin",
        "target_table": "public.tbltmprecyclebin",
        "conflict_columns": ["idbin"],
        "method": "upsert",
    },
    {
        "name": "tbluploadzip",
        "sql": "SELECT id_tbluploadzip, owner, id_folder, folder, date FROM adele.tbluploadzip",
        "target_table": "public.tbluploadzip",
        "conflict_columns": ["id_tbluploadzip"],
        "method": "upsert",
    },
    {
        "name": "tbluseraktivitas",
        "sql": "SELECT id, document, added, owner FROM adele.tbluseraktivitas",
        "target_table": "public.tbluseraktivitas",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbluserimages",
        "sql": "SELECT id, userid, image, mimetype FROM adele.tbluserimages",
        "target_table": "public.tbluserimages",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tbluserlogs",
        "sql": 'SELECT userlogid, "user", loginat, ip FROM adele.tbluserlogs',
        "target_table": "public.tbluserlogs",
        "conflict_columns": ["userlogid"],
        "method": "upsert",
    },
    {
        "name": "tbluserparaf",
        "sql": "SELECT id, userid, image, mimetype FROM adele.tbluserparaf",
        "target_table": "public.tbluserparaf",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblusers",
        "sql": "SELECT id, login, pwd, fullname, handphone, email, language, theme, comment, isadmin, isbypass, hidden, isldap, isdepthead, isactivated, pendingdelete, createdby, delegatedto, lastlogin, iswh, isrecept, isdir, issu, isbiro, sekdir, ispro, isgm, iskom, sesdekom, fcmapi FROM adele.tblusers",
        "target_table": "public.tblusers",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblusersign",
        "sql": "SELECT id, userid, image, mimetype FROM adele.tblusersign",
        "target_table": "public.tblusersign",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
    {
        "name": "tblworkflow",
        "sql": "SELECT tblworkflowid, owner, tblworkflowname, tblworkflowdescription, tblworkflowdate, tblworkflowtype, templategroup FROM adele.tblworkflow",
        "target_table": "public.tblworkflow",
        "conflict_columns": ["tblworkflowid"],
        "method": "upsert",
    },
    {
        "name": "tblworkflowchild",
        "sql": 'SELECT tblworkflowchildid, tblworkflowid, tblworkflowstatus, userid, "order" FROM adele.tblworkflowchild',
        "target_table": "public.tblworkflowchild",
        "conflict_columns": ["tblworkflowchildid"],
        "method": "upsert",
    },
    {
        "name": "tmpmigrasi",
        "sql": "SELECT id, ref_number, prefix, year, dept_pengirim, folder, folder_path, owner, tujuan, dest_unit, dest_name, perihal, tgl_surat, has_attachment, file_path, file_path_dms, upload_date, updatedat, plain_folder, plain_folder_path FROM adele.tmpmigrasi",
        "target_table": "public.tmpmigrasi",
        "conflict_columns": ["id"],
        "method": "upsert",
    },
]


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--source_type", required=True)
    p.add_argument("--source_host", required=True)
    p.add_argument("--source_port", required=True)
    p.add_argument("--source_db", required=True)
    p.add_argument("--source_user", required=True)
    p.add_argument("--source_password", required=True)
    p.add_argument("--target_type", required=True)
    p.add_argument("--target_host", required=True)
    p.add_argument("--target_port", required=True)
    p.add_argument("--target_db", required=True)
    p.add_argument("--target_user", required=True)
    p.add_argument("--target_password", required=True)
    return p.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("Universal-Spark-ETL").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    source_cfg = {
        "jdbc_url": build_jdbc_url(
            args.source_type, args.source_host, args.source_port, args.source_db
        ),
        "user": args.source_user,
        "password": args.source_password,
        "driver": {
            "postgres": "org.postgresql.Driver",
            "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "mariadb": "org.mariadb.jdbc.Driver",
            "oracle": "oracle.jdbc.driver.OracleDriver",
        }[args.source_type],
    }

    target_cfg = {
        "jdbc_url": build_jdbc_url(
            args.target_type, args.target_host, args.target_port, args.target_db
        ),
        "user": args.target_user,
        "password": args.target_password,
        "driver": {
            "postgres": "org.postgresql.Driver",
            "mssql": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "mariadb": "org.mariadb.jdbc.Driver",
            "oracle": "oracle.jdbc.driver.OracleDriver",
        }[args.target_type],
    }

    summary = {"success": 0, "failed": 0}

    for job in ETL_JOBS:
        print(f"▶ {job['name']}")
        try:
            df = jdbc_read(spark, source_cfg, job["sql"])

            target_cfg["table"] = job["target_table"]

            stats, data_success, data_failed = run_method(
                spark,
                df,
                target_cfg,
                job["method"],
                job.get("conflict_columns"),
                transaction=job.get("transaction", False),
            )

            print(f"  Stats: {stats}")
            print(f"  Data Success: {len(data_success)} rows")
            print(f"  Data Failed: {len(data_failed)} rows")
            summary["success"] += 1

        except Exception as e:
            print(f"❌ {job['name']} failed: {e}")
            summary["failed"] += 1
            spark.stop()
            sys.exit(1)

    spark.stop()
    print("SUMMARY:", summary)


if __name__ == "__main__":
    main()
