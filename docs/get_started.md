# Gebruikershandleiding


## Downloaden data
 - Download de bestanden van Deltares-ftp in `path\naar\de\repos\data`. Zie voor de juiste structuur en beschrijving: [https://github.com/d2hydro/lhm-ribasim/tree/main/data](https://github.com/d2hydro/lhm-ribasim/tree/main/data)

 ## Runnen test-model
 - Installeer RIBASIM volgens de [handleiding](https://deltares.github.io/Ribasim/core/usage.html). Unzip de inhoud van ribasim_cli.zip in de root van de resporitory, `path\naar\de\repos\ribasim_cli`.
 - Run het test-model in een cmd met `path\naar\de\repos\scripts\run_test_model.cmd`. Dit levert het volgende resultaat:

 ![](images/run_ribasim_test.png "Run Ribasim")

 ## Opzetten Python environment
 - Bouw je environment met [https://github.com/d2hydro/lhm-ribasim/blob/main/envs/environment_dev_spyder.yml](https://github.com/d2hydro/lhm-ribasim/blob/main/envs/environment_dev_spyder.yml)
- Clone de Deltares RIBASIM repository: [https://github.com/Deltares/Ribasim](https://github.com/Deltares/Ribasim)
- Run in de geactiveerde conda-environment `pip install -e .` in de ribasim repository in de sub-folder `python\ribasim` (je vindt daar en pyproject.toml file)
