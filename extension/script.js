const tabs = document.querySelector("#content .nav-tabs")

const el = document.createElement("a")
el.innerText = "Data Explorer"
el.href = "#"
el.id = "explorer-link-injected"

tabs.appendChild(el)
