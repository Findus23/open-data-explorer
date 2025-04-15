const tabs = document.querySelector("#content .nav-tabs")

const datasetID = window.location.pathname.split("/").at(-1)

const el = document.createElement("a")
el.innerText = "Data Explorer"
el.href = "https://ode.localhost/meta/" + datasetID + "/fetch"
el.id = "explorer-link-injected"

tabs.appendChild(el)
