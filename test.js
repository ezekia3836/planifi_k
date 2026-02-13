fetch("https://konticreav2.kontikimedia.fr:5009/api/creativities/filter-plannifik?focus_id=384141&base_id=62", {
  method: "POST"
})
.then(res => res.json())
.then(data => console.log("Réponse:", data))
.catch(err => console.error("Erreur:", err));