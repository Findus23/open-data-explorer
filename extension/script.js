const tabs = document.querySelector("#content .nav-tabs")

const datasetID = window.location.pathname.split("/").at(-1)

const el = document.createElement("a")
el.innerText = "Data Explorer"
el.href = "http://127.0.0.1:8000/meta/" + datasetID + "/fetch"
el.id = "explorer-link-injected"

tabs.appendChild(el)
