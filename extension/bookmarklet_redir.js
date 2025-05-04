const loc = window.location
const currentURL=loc.pathname
if (currentURL.startsWith("https://www.data.gv.at/katalog/dataset/")) {
    const datasetID = currentURL.split("/").at(-1)

    loc.assign("https://ode.localhost/meta/" + datasetID)
}
