#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

/*
=============================================================================================================================
    LOAD FILE RETENTION SCHEMES
=============================================================================================================================
*/
def retention_schema = new ConfigSlurper().parse( file(params.retention_schema).text )

/*
=============================================================================================================================
    GET TIMESTAMP
=============================================================================================================================
*/
Date now = new Date()
long now_unix = now.getTime() / 1000
println "Files will be saved with timestamp: ${now_unix}"

/*
=============================================================================================================================
    PREPARE OUTPUTS
=============================================================================================================================
*/
// Log File
def log_name = "${now_unix}-waphl-prod2res.log"
def log_tmp = file(workflow.workDir).resolve(log_name)
def log_file = file(params.outdir).resolve("logs").resolve(log_name)
log_tmp.text = "Note: If this message is not updated it means the workflow failed."
log_tmp.moveTo(log_file)

// Meta File
def meta_tmp  = file(workflow.workDir).resolve("${now_unix}.csv")
def meta_file = file(params.outdir).resolve("meta").resolve("${now_unix}.csv")

/*
=============================================================================================================================
    DETERMINE TIMESPAN
=============================================================================================================================
*/
if( ! (params.delta || params.start || params.end) ){ exit 1, "ERROR: Provide a timespan using `--delta` or `--start` and `--end`" }
if( params.delta && (params.start || params.end) ){ exit 1, "ERROR: Provide a timespan using either `--delta` or `--start` and `--end` - not both!" }
if( ( params.start && ! params.end ) || ( ! params.start && params.end ) ){ exit 1, "ERROR: Both `--start` and `--end` must be specifed when not using `--delta`" }
if( params.end < params.start ){ exit 1, "ERROR: `--start` must be smaller than `--end`." }
def start_time = params.start ? params.start : now_unix - params.delta
def end_time = params.end ? params.end : now_unix

workflow {

    /*
    =============================================================================================================================
        IDENTIFY FILES
    =============================================================================================================================
    */
    Channel
        .fromPath(file(params.input))
        .splitCsv(header: true)
        .map{ it -> [ it.workflow, file(it.path).listFiles().toList() ] }
        .transpose()
        .map{ workflow, run -> [ workflow: workflow, 
                                 run: run, 
                                 timestamp: file(run).lastModified() / 1000, 
                                 directory: file(run).isDirectory() ? true : false, 
                                 manifest: run.resolve("manifest.csv").exists() ? run.resolve("manifest.csv") : null ] }
        .filter{ it.directory && it.timestamp >= start_time && it.timestamp <= end_time && it.manifest }
        .map{ [ it.run, it.workflow, it.timestamp, apply_schema(it.run, it.workflow, it.manifest, retention_schema) ] }
        .transpose()
        .map{ run, workflow, timestamp, files -> [ run: run, workflow: workflow, timestamp: timestamp, sample: files.sample, fileorigin: file(files.file), filename: file(files.file).getName() ] }
        .map{  it + [ filedest: file(params.outdir, checkIfExists: false ) / "data" / "id=${it.sample}" / "workflow=${it.workflow}" / "run=${it.run}" / "file=${it.filename}" / "timestamp=${it.timestamp}" / it.filename ] }
        .map{ it + [ strorigin: pathToString(it.fileorigin), strdest: pathToString(it.filedest) ] }
        .unique()
        .set{ ch_files }

    /*
    =============================================================================================================================
        TRANSFER FILES
    =============================================================================================================================
    */

    // THROTTLE (
    //     ch_files.collate(params.batchsize)
    // )

    // THROTTLE
    //     .out
    //     .files
    //     .flatten()
    //     .set{ ch_th_files }
    
    // // ch_th_files.subscribe{ file(it.fileorigin).copyTo(file(it.filedest)) }

    // Channel
    //     .of("id,workflow,run,file,timestamp,origin,current")
    //     .concat(ch_th_files.map{ "${it.sample},${it.workflow},${it.run},${it.filename},${it.timestamp},${it.strorigin},${it.strdest}" })
    //     .collect()
    //     .subscribe{ meta_tmp.text = it.join('\n')
    //                 if(count_lines(meta_tmp) > 1){meta_tmp.copyTo(meta_file)} }
        
}

workflow.onComplete {

    def file_count = count_lines(meta_tmp)
    meta_file  = file_count > 1 ? meta_file : "None"

    def msg = """\
        Pipeline execution summary
        ---------------------------
        Timestamp         : ${now_unix}
        Log file          : ${log_file}
        Metadata file     : ${meta_file}
        Files transferred : ${file_count - 1}
        Completed at      : ${workflow.complete}
        Duration          : ${workflow.duration}
        Success           : ${workflow.success}
        Exit status       : ${workflow.exitStatus}
        """
        .stripIndent()
    
    println msg
    
    log_file.text = msg

    if(params.email){sendMail(to: params.email, subject: "waphl-prod2res: ${now_unix}", body: msg)}
}

/*
=============================================================================================================================
    FUNCTIONS
=============================================================================================================================
*/
def apply_schema(run_dir, workflow, manifest, schemes){
    def run_path     = pathToString(run_dir)
    def schema       = schemes[workflow]
    def manifest_csv = file(manifest).splitCsv(header: true)
    def read1        = manifest_csv.collect{ [ sample: it.sample, file: file(it.fastq_1).exists() ? it.fastq_1 : null ] }
    def read2        = manifest_csv.collect{ [ sample: it.sample, file: file(it.fastq_2).exists() ? it.fastq_2 : null ] }
    def sample_files = schema.sample_files.collectMany{ f -> manifest_csv.sample.collect{ sample -> [ sample: sample, file: run_path + f.replace("<sample>", sample) ] } }
    def files        = sample_files + schema.run_files.collect{ f -> [ sample: null, file: run_path + f ] }
                           .collect{ [ sample: it.sample, file: file(it.file).exists() ? it.file : null ] }
    files            = files + read1 + read2
    return files.findAll{ it.file }
}

// convert to string to path, retaining the schema
def pathToString(path){
    def schema = path.getScheme()
    def pathstring = path.toString()
    def result = path.getScheme() == "file" ? pathstring : "${schema}:/${pathstring}"
    return result
}

// count lines in a file
def count_lines(myFile){
    count = 0
    myFile.eachLine { str -> count++ }
    return count
}

/*
=============================================================================================================================
    MODULES
=============================================================================================================================
*/

process THROTTLE {
    maxForks 1

    input:
    val it

    output:
    val it, emit: files

    script:
    """
    sleep 10
    """

}

