fetch("https://konticreav2.kontikimedia.fr:5009/api/creativities/filter-plannifik?focus_id=391529&base_id=118", {
  method: "POST"
})
.then(res => res.json())
.then(data => console.log("Réponse:", data))
.catch(err => console.error("Erreur:", err));