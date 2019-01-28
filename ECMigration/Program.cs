using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using McMaster.Extensions.CommandLineUtils;
using System.Linq;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using System.Globalization;

namespace ECMigration
{
    public struct FieldInfo
    {
        public int Index { get; set; }
        public string Type { get; set; }
        public int Length { get; set; }
        public string Description { get; set; }
    }
    public class Program
    {
        public async static Task<int> Main(string[] args)
        => await CommandLineApplication.ExecuteAsync<Program>(args);

        [Argument(0, Description = "The definiton file")]
        public string DefinitionFile { get; }

        [Option(ShortName = "D")]
        public string UnloadDir { get; }
        [Option(ShortName = "M")]
        public string MappingDir { get; }

        [Option(ShortName = "O")]
        public string OutputDir { get; }

        [Option(ShortName = "T")]
        public string TableDir { get; }

        protected Dictionary<string, Dictionary<string, Dictionary<string, FieldInfo>>> fieldMap = new Dictionary<string, Dictionary<string, Dictionary<string, FieldInfo>>>();

        protected Dictionary<string, Dictionary<string, List<List<string>>>> valueMap = new Dictionary<string, Dictionary<string, List<List<string>>>>();

        protected Dictionary<string, Dictionary<string, List<KeyValuePair<string, int>>>> entities = new Dictionary<string, Dictionary<string, List<KeyValuePair<string, int>>>>();

        protected Dictionary<string, JArray> mappings = new Dictionary<string, JArray>();

        protected JObject tableMappings = new JObject();

        private bool testIf(JObject prop, string obj,List<KeyValuePair<string,int>> entity)
        {
            if (prop.Property("_if") != null)
            {
                var ifQuery = prop.Property("_if").Value.Value<string>();
                var ifParts = ifQuery.Split("=");
                var testValue = getPropertyFromPath(obj, entity, ifParts[0]);
                return testValue == ifParts[1];
            }
            return true;
        }
        private string evaluateFunction(string obj, List<KeyValuePair<string, int>> entity, string func)
        {
            var openBracketIndex = func.Substring(1).IndexOf("(") + 1;
            var closeBracketIndex = func.Substring(1).LastIndexOf(")");
            var functionName = func.Substring(1, openBracketIndex - 1);

            var functionParams = func.Substring(openBracketIndex + 1, closeBracketIndex - openBracketIndex).Split(",");
            if (functionName == "timestamp")
            {
                var dateTime = getPropertyFromPath(obj, entity, functionParams.First());
                if (DateTime.TryParseExact(dateTime, "yyyyMMdd", new CultureInfo("de-DE"), DateTimeStyles.None, out DateTime date))
                {
                    return new DateTimeOffset(date).ToUnixTimeMilliseconds().ToString();
                }
                else
                {
                    return dateTime;
                }
            }
            else if (functionName == "map")
            {
                var propVal = getPropertyFromPath(obj, entity, functionParams.First());
                try
                {
                    var mappingObject = tableMappings[functionParams.Last()].Value<JObject>();
                    return mappingObject[propVal].Value<string>();
                }
                catch (Exception)
                {
                    return propVal;
                }
            }
            else if (functionName == "concat")
            {
                return String.Join(functionParams.First(), functionParams.Skip(1).Select(param => getPropertyFromPath(obj, entity, param)));
            }
            return "unknown function " + func;
        }
        private string getPropertyFromPath(string obj, List<KeyValuePair<string, int>> entity, string path)
        {
            if (path.First() != '$')
            {
                if (path.First() == '§')
                {
                    return evaluateFunction(obj, entity, path);
                }
                else
                {
                    return path;
                }
            }
            var parts = path.Substring(1).Split("-");

            if (parts[0].Contains("["))
            {
                //get separate entity
                var key = path.Split("[").Last().Split(']').First();

                var entityKey = getPropertyFromPath(obj, entity, key);
                entity = entities[parts[0].Split("[").First()][entityKey];

                //skip till the closing bracket
                parts = path.Split("[").Last().Split(']').Last().Split("-", StringSplitOptions.RemoveEmptyEntries);
                //redefine the obj
                obj = path.Split("[").First().Substring(1);
            }
            var partIndex = entity.Where(e => e.Key == parts[0]).FirstOrDefault().Value;
            var fieldIndex = fieldMap[obj][parts[0]][parts[1]].Index;
            return valueMap[obj][parts[0]][partIndex][fieldIndex];
        }

        private void mapObject(string obj, JProperty prop, JObject targetObject, List<KeyValuePair<string, int>> entity)
        {
            
            if (prop.Value is JObject)
            {
                var type = "object";
                if ((prop.Value as JObject).Property("_type") != null)
                {
                    type = (prop.Value as JObject).Property("_type").Value.Value<string>();
                }
                if (type == "object")
                {
                    if (!testIf(prop.Value as JObject,obj,entity))
                        return;
                    var newObj = new JObject();
                    foreach (var subProp in (prop.Value as JObject).Properties())
                    {
                        mapObject(obj, subProp, newObj, entity);
                    }
                    targetObject.Add(prop.Name, newObj);
                }
                else if (type == "array")
                {
                    var entityType = (prop.Value as JObject).Property("_entity").Value.Value<string>();
                    var mappedName = prop.Name;
                    if ((prop.Value as JObject).Property("_rename") != null)
                    {
                        mappedName = type = (prop.Value as JObject).Property("_rename").Value.Value<string>();
                    }
                    var requires = new JArray();
                    if ((prop.Value as JObject).Property("_requires") != null)
                    {
                        requires = (prop.Value as JObject).Property("_requires").Value.Value<JArray>();
                    }


                    var newArray = new JArray();

                    foreach (var newEntity in entity.Where(e => e.Key == entityType))
                    {
                        JObject newObj = new JObject();
                        foreach (var subProp in (prop.Value as JObject).Properties())
                        {
                            var skip = false;
                            foreach (var require in requires)
                            {
                                if (String.IsNullOrWhiteSpace(getPropertyFromPath(obj, new List<KeyValuePair<string, int>>() { newEntity }, require.Value<string>())))
                                {
                                    skip = true;
                                    break;
                                }
                            }
                            if (skip)
                                continue;
                            mapObject(obj, subProp, newObj, new List<KeyValuePair<string, int>>() { newEntity });
                        }
                        if (newObj.Properties().Count() > 0)
                            newArray.Add(newObj);
                    }
                    if (targetObject.Property(mappedName) == null)
                    {
                        targetObject.Add(mappedName, newArray);
                    }
                    else
                    {
                        (targetObject[mappedName] as JArray).Merge(newArray);
                    }
                }


            }
            else if (prop.Value is JArray)
            {
                // do nothing, should not happen
            }
            else if (prop.Value is JToken)
            {
                if (prop.Name.StartsWith("_"))
                    return;
                if (prop.Value.Value<string>().StartsWith("$"))
                {
                    //find value
                    targetObject.Add(prop.Name, getPropertyFromPath(obj, entity, prop.Value.Value<string>()));
                }
                else if (prop.Value.Value<string>().StartsWith("§")) //predefined functions
                {

                    targetObject.Add(prop.Name, evaluateFunction(obj, entity, prop.Value.Value<string>()));

                }
                else
                {
                    targetObject.Add(prop);
                }
            }
        }
        private async Task OnExecuteAsync()
        {
            if (DefinitionFile != null)
            {
                var lines = await System.IO.File.ReadAllLinesAsync(DefinitionFile);
                int subObjIndex = 0;
                foreach (var line in lines.Skip(1))
                {
                    var parts = line.Split("\t");
                    var obj = parts[3].Trim();
                    if (!fieldMap.ContainsKey(obj))
                    {
                        fieldMap.Add(obj, new Dictionary<string, Dictionary<string, FieldInfo>>());
                    }
                    var subObj = parts[7].Trim();

                    if (!fieldMap[obj].ContainsKey(subObj))
                    {
                        fieldMap[obj].Add(subObj, new Dictionary<string, FieldInfo>());
                        subObjIndex = -1;
                    }
                    var field = parts[9].Trim();
                    subObjIndex++;
                    if (!fieldMap[obj][subObj].ContainsKey(field))
                    {
                        fieldMap[obj][subObj].Add(field, new FieldInfo() { Index = subObjIndex, Type = parts[10], Length = Int32.Parse(parts[11]), Description = parts[13] });
                    }

                }
                var files = System.IO.Directory.EnumerateFiles(this.UnloadDir);
                foreach (var file in files)
                {

                    var obj = System.IO.Path.GetFileNameWithoutExtension(file).Split('-').Last();
                    if (!valueMap.ContainsKey(obj))
                    {
                        valueMap.Add(obj, new Dictionary<string, List<List<string>>>());
                    }
                    if (!entities.ContainsKey(obj))
                    {
                        entities.Add(obj, new Dictionary<string, List<KeyValuePair<string, int>>>());
                    }

                    foreach (var line in (await System.IO.File.ReadAllLinesAsync(System.IO.Path.Combine(this.UnloadDir, file))).Skip(3))
                    {
                        var lineParts = line.Split("\t");
                        var entityKey = lineParts[0].Trim();
                        var subObj = lineParts[1].Trim();
                        if (!entities[obj].ContainsKey(entityKey))
                        {
                            entities[obj].Add(entityKey, new List<KeyValuePair<string, int>>());
                        }
                        if (!valueMap[obj].ContainsKey(subObj))
                        {
                            valueMap[obj].Add(subObj, new List<List<string>>());
                        }
                        valueMap[obj][subObj].Add(lineParts.Skip(2).ToList());
                        entities[obj][entityKey].Add(new KeyValuePair<string, int>(subObj, valueMap[obj][subObj].Count - 1));
                    }
                }
                var mappingFiles = System.IO.Directory.EnumerateFiles(this.MappingDir);
                foreach (var file in mappingFiles)
                {
                    var obj = System.IO.Path.GetFileNameWithoutExtension(file).Split('-').Last();
                    mappings.Add(obj, JArray.Parse(await System.IO.File.ReadAllTextAsync(System.IO.Path.Combine(this.MappingDir, file))));
                }

                var tableMappingFiles = System.IO.Directory.EnumerateFiles(this.TableDir);
                foreach (var file in tableMappingFiles)
                {
                    var obj = System.IO.Path.GetFileNameWithoutExtension(file).Split('-').Last();
                    tableMappings.Merge(JObject.Parse(await System.IO.File.ReadAllTextAsync(System.IO.Path.Combine(this.TableDir, file))));
                }
                // perform mappings manually right now
                JArray customers = new JArray();
                foreach (var bp in entities["P2A"])
                {
                    foreach (JObject map in mappings["customer"])
                    {
                        if (!testIf(map, "P2A", bp.Value))
                            continue;
                        JObject newCust = new JObject();
                        foreach (var prop in map.Properties())
                        {
                            mapObject("P2A", prop, newCust, bp.Value);
                        }
                        customers.Add(newCust);
                    }
                }
                await System.IO.File.WriteAllTextAsync(System.IO.Path.Combine(this.OutputDir, "customers.json"), JsonConvert.SerializeObject(customers));

                JArray individuals = new JArray();
                foreach (var bp in entities["P2A"])
                {
                    foreach (JObject map in mappings["individuals"])
                    {
                        if (!testIf(map, "P2A", bp.Value))
                            continue;
                        JObject newIndividual = new JObject();
                        foreach (var prop in map.Properties())
                        {
                            mapObject("P2A", prop, newIndividual, bp.Value);
                        }
                        individuals.Add(newIndividual);
                    }
                }
                await System.IO.File.WriteAllTextAsync(System.IO.Path.Combine(this.OutputDir, "individuals.json"), JsonConvert.SerializeObject(individuals));

                JArray billingAccounts = new JArray();
                foreach (var vkk in entities["ACC"])
                {
                    foreach (JObject map in mappings["billingAccounts"])
                    {

                        if (map["_baseObject"].Value<string>() != "ACC")
                            continue;
                        if (!testIf(map, "ACC", vkk.Value))
                            continue;
                        JObject newAccount = new JObject();
                        foreach (var prop in map.Properties())
                        {
                            mapObject("ACC", prop, newAccount, vkk.Value);
                        }
                        billingAccounts.Add(newAccount);
                    }
                }
                foreach (var vkk in entities["MOI"])
                {
                    foreach (JObject map in mappings["billingAccounts"])
                    {
                        if (map["_baseObject"].Value<string>() != "MOVE_IN")
                            continue;
                        if (!testIf(map, "MOI", vkk.Value))
                            continue;
                        JObject newAccount = new JObject();
                        foreach (var prop in map.Properties())
                        {
                            mapObject("MOI", prop, newAccount, vkk.Value);
                        }
                        billingAccounts.Add(newAccount);
                    }
                }
                await System.IO.File.WriteAllTextAsync(System.IO.Path.Combine(this.OutputDir, "billingAccounts.json"), JsonConvert.SerializeObject(billingAccounts));

                JArray products = new JArray();
                foreach (var contract in entities["MOI"])
                {
                    foreach (JObject map in mappings["product"])
                    {
                        if (!testIf(map, "MOI", contract.Value))
                            continue;
                        JObject newProduct = new JObject();
                        foreach (var prop in map.Properties())
                        {
                            mapObject("MOI", prop, newProduct, contract.Value);
                        }
                        products.Add(newProduct);
                    }
                }
                await System.IO.File.WriteAllTextAsync(System.IO.Path.Combine(this.OutputDir, "products.json"), JsonConvert.SerializeObject(products));

                Console.WriteLine(lines.Length);
            }
        }
    }
}
