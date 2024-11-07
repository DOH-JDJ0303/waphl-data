#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

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
        LOAD MANIFEST
    =============================================================================================================================
    */
    Channel
        .fromPath(file(params.input))
        .splitCsv(header: true)
        .map{ it -> [ it.workflow, file(it.path).listFiles().toList() ] }
        .transpose()
        .map{ workflow, run -> [ workflow: workflow, run: run, timestamp: file(run).lastModified() / 1000, directory: file(run).isDirectory() ? true : false ] }
        .filter{ it.directory && it.timestamp >= start_time && it.timestamp <= end_time }
        .set{ ch_runs }

    ch_runs
        .filter{ it.workflow == "phoenix" }
        .map{ [ it.run, it.timestamp, it.run.listFiles().toList() ] }
        .transpose()
        .map{ run, timestamp, sample -> [ run: run, timestamp: timestamp, sample: sample ] }
        .filter{ file(it.sample).isDirectory() && ! ["pipeline_info","reads"].contains(it.sample.toString().tokenize("/")[-1]) }
        .map{ [ it.run, it.timestamp, it.sample, it.run.resolve("manifest.csv") ] }
        .splitCsv(header: true, elem: 3)
        .map{ run, timestamp, sample, manifest -> [ run, timestamp, sample, manifest.each{ manifest.sample == sample.toString().tokenize("/")[-1] } ] }
        .map{ run, timestamp, sample, manifest -> [ sample, timestamp, [ file(manifest.fastq_1), file(manifest.fastq_2) , run.resolve("pipeline_info").listFiles().toList(), collect_files(sample) ].flatten() ] }
        .transpose()
        .map{ sample, timestamp, f -> [ sample: sample.toString().tokenize("/")[-1], workflow: "phoenix", timestamp: timestamp, filename: file(f).getName(), strorigin: pathToString(file(f)), fileorigin: f  ] }
        .map{  it + [ filedest: file(params.outdir) / "data" / "id=${it.sample}" / "workflow=${it.workflow}" / "file=${it.filename}" / "timestamp=${it.timestamp}" / it.filename ] }
        .map{  it + [ strdest: pathToString(it.filedest) ] }
        .set{ ch_phoenix }

    ch_phoenix
        .collate(params.batchsize)
        .set{ ch_files }

    THROTTLE (
        ch_files
    )

    THROTTLE
        .out
        .files
        .flatten()
        .set{ ch_th_files }
    
    ch_th_files.subscribe{ file(it.fileorigin).copyTo(file(it.filedest)) }

    meta_file = file(workflow.workDir).resolve("${now_unix}.csv")
    Channel
        .of("id,workflow,file,timestamp,origin,current")
        .concat(ch_th_files.map{ "${it.sample},${it.workflow},${it.filename},${it.timestamp},${it.strorigin},${it.strdest}" })
        .collect()
        .subscribe{ meta_file.text = it.join('\n')
                    meta_file.copyTo(file(params.outdir).resolve("meta").resolve("${now_unix}.csv")) }
        
}

/*
=============================================================================================================================
    FUNCTIONS
=============================================================================================================================
*/
// get list of files recursivley from directory
def collect_files(dir){
    def fileList = []
    file(dir).eachFileRecurse { f -> 
                                if(f.isFile()){
                                    fileList << f 
                                }
                            }
    return fileList
}

// convert to string to path, retaining the schema
def pathToString(path){
    def schema = path.getScheme()
    def pathstring = path.toString()
    def result = path.getScheme() == "file" ? pathstring : "${schema}:/${pathstring}"
    return result
}

/*
=============================================================================================================================
    MODULES
=============================================================================================================================
*/

process THROTTLE {

    input:
    val it

    output:
    val it, emit: files

    script:
    """
    sleep 10
    """

}

