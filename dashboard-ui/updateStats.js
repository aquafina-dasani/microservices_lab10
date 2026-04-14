/* UPDATE THESE VALUES TO MATCH YOUR SETUP */

const IP = "localhost"; // localhost OR ec2 public IP

const PROCESSING_STATS_API_URL = "/processing/stats"
const ANALYZER_API_URL = {
    stats: "/analyzer/stats",
    sectors: "/analyzer/race/sectors?index=0",
    laps: "/analyzer/race/laps?index=0"
}

// This function fetches and updates the general statistics
const makeReq = (url, cb) => {
    fetch(url)
        .then(res => res.json())
        .then((result) => {
            console.log("Received data: ", result)
            cb(result);
        }).catch((error) => {
            updateErrorMessages(error.message)
        })
}


const updateCodeDiv = (result, elemId) => {
    const elem = document.getElementById(elemId);
    if (elem) {
        // null, 2 adds 2 spaces of indentation for "pretty printing"
        elem.innerText = JSON.stringify(result, null, 2);
    }
}

const getLocaleDateStr = () => (new Date()).toLocaleString()

const getStats = () => {
    document.getElementById("last-updated-value").innerText = getLocaleDateStr()
    
    makeReq(PROCESSING_STATS_API_URL, (result) => updateCodeDiv(result, "processing-stats"))
    makeReq(ANALYZER_API_URL.stats, (result) => updateCodeDiv(result, "analyzer-stats"))
    makeReq(ANALYZER_API_URL.sectors, (result) => updateCodeDiv(result, "event-sectors"))
    makeReq(ANALYZER_API_URL.laps, (result) => updateCodeDiv(result, "event-laps"))
}

const updateErrorMessages = (message) => {
    const id = Date.now()
    console.log("Creation", id)
    msg = document.createElement("div")
    msg.id = `error-${id}`
    msg.innerHTML = `<p>Something happened at ${getLocaleDateStr()}!</p><code>${message}</code>`
    document.getElementById("messages").style.display = "block"
    document.getElementById("messages").prepend(msg)
    setTimeout(() => {
        const elem = document.getElementById(`error-${id}`)
        if (elem) { elem.remove() }
    }, 7000)
}

const setup = () => {
    getStats()
    setInterval(() => getStats(), 4000) // Update every 4 seconds
}

document.addEventListener('DOMContentLoaded', setup)